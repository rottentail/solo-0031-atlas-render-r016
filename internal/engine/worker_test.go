package engine

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func validDocument() model.Document {
	return model.Document{
		Width: 4, Height: 4, Background: "#000000",
		Layers: []model.Layer{{
			ID: "plate", Kind: "rectangle", Width: 2, Height: 2, Opacity: 1,
			Transform: model.Transform{ScaleX: 1, ScaleY: 1},
			Fill:      model.Paint{Kind: "solid", Color: "#FFFFFF"},
		}},
	}
}

// seed inserts a scene, snapshot and job directly, mirroring what the service
// would have committed before the worker picks the job up.
func seed(t *testing.T, store *persist.Store, state string) model.Job {
	t.Helper()
	var job model.Job
	e := store.Update(func(s *model.State) error {
		doc := validDocument()
		s.Scenes["scene-1"] = model.Scene{ID: "scene-1", Name: "scene", Revision: 1, State: "active", Document: doc}
		snap := model.Snapshot{ID: "snap-1", SceneID: "scene-1", Revision: 1, State: "active", Document: doc, Assets: map[string]model.Asset{}}
		snap.Digest = model.SnapshotDigest(snap)
		s.Snapshots["snap-1"] = snap
		job = model.Job{ID: "job-1", SnapshotID: "snap-1", State: state, Scale: 1, Attempt: 1, Payload: snap}
		s.Jobs[job.ID] = job
		return nil
	})
	if e != nil {
		t.Fatalf("seed: %v", e)
	}
	return job
}

func openEngine(t *testing.T, path string, writer persist.Writer) (*persist.Store, *Worker) {
	t.Helper()
	store, e := persist.OpenWith(path, writer)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	t.Cleanup(func() { store.Close() })
	return store, New(store)
}

// A cancel that committed during rendering must win; the finished render must
// not overwrite the job with success.
func TestStaleAttemptCannotOverwriteNewerFact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, worker := openEngine(t, path, persist.AtomicWrite)
	attempt := seed(t, store, "running")

	// Simulate a cancel committing while the attempt is still rendering.
	if e := store.Update(func(s *model.State) error {
		j := s.Jobs[attempt.ID]
		j.State = "cancelled"
		s.Jobs[j.ID] = j
		return nil
	}); e != nil {
		t.Fatalf("cancel: %v", e)
	}

	result := worker.render(context.Background(), attempt)
	if result.err != nil {
		t.Fatalf("render: %v", result.err)
	}
	if e := worker.record(attempt, result); e != nil {
		t.Fatalf("record: %v", e)
	}
	j := store.Read().Jobs[attempt.ID]
	if j.State != "cancelled" {
		t.Fatalf("state = %q, want cancelled", j.State)
	}
	if len(store.Read().Artifacts) != 0 {
		t.Fatal("stale attempt stored an artifact after cancellation")
	}
}

// A retry that bumped the attempt number must reject the older attempt's
// completion.
func TestOldAttemptNumberIsIsolated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, worker := openEngine(t, path, persist.AtomicWrite)
	attempt := seed(t, store, "running")
	if e := store.Update(func(s *model.State) error {
		j := s.Jobs[attempt.ID]
		j.State = "queued"
		j.Attempt = 2
		s.Jobs[j.ID] = j
		return nil
	}); e != nil {
		t.Fatalf("retry: %v", e)
	}
	result := worker.render(context.Background(), attempt)
	if result.err != nil {
		t.Fatalf("render: %v", result.err)
	}
	if e := worker.record(attempt, result); e != nil {
		t.Fatalf("record: %v", e)
	}
	if j := store.Read().Jobs[attempt.ID]; j.State != "queued" || j.Attempt != 2 {
		t.Fatalf("old attempt changed state: %+v", j)
	}
}

// If the result commit fails before it lands, the job must stay running rather
// than appear succeeded, so recovery can retry it.
func TestFailedResultCommitLeavesJobRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var failNext bool
	writer := func(p string, d []byte) error {
		if failNext {
			failNext = false
			return &persist.WriteError{Stage: persist.StageSync, Err: errors.New("injected fsync failure")}
		}
		return persist.AtomicWrite(p, d)
	}
	store, worker := openEngine(t, path, writer)
	attempt := seed(t, store, "running")
	result := worker.render(context.Background(), attempt)
	if result.err != nil {
		t.Fatalf("render: %v", result.err)
	}
	failNext = true
	e := worker.record(attempt, result)
	if e == nil {
		t.Fatal("expected commit failure")
	}
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q", got)
	}
	state := store.Read()
	if state.Jobs[attempt.ID].State != "running" {
		t.Fatalf("failed commit changed job state to %q", state.Jobs[attempt.ID].State)
	}
	if len(state.Artifacts) != 0 {
		t.Fatal("failed commit stored a partial artifact")
	}
	// A later commit succeeds and records both facts together.
	if e := worker.record(attempt, result); e != nil {
		t.Fatalf("retry record: %v", e)
	}
	state = store.Read()
	if state.Jobs[attempt.ID].State != "succeeded" {
		t.Fatalf("state = %q, want succeeded", state.Jobs[attempt.ID].State)
	}
	if len(state.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want 1", len(state.Artifacts))
	}
	if _, ok := state.Artifacts[state.Jobs[attempt.ID].ArtifactID]; !ok {
		t.Fatal("succeeded job references a missing artifact")
	}
}

// A claimed job that disappears must produce an internal error, not a silent
// success that leaves the caller believing the record was written.
func TestRecordRejectsMissingJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, worker := openEngine(t, path, persist.AtomicWrite)
	attempt := seed(t, store, "running")
	if e := store.Update(func(s *model.State) error {
		delete(s.Jobs, attempt.ID)
		return nil
	}); e != nil {
		t.Fatalf("remove job: %v", e)
	}
	result := worker.render(context.Background(), attempt)
	if e := worker.record(attempt, result); e == nil {
		t.Fatal("expected an error for a missing job")
	} else if kind, _ := model.KindOf(e); kind != model.KindInternal {
		t.Fatalf("kind = %q, want internal", kind)
	}
}
