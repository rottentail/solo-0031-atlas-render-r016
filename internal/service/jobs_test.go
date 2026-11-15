package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// scriptedWriter applies a per-call plan. Nil entries perform the real atomic
// write; non-nil entries simulate a specific storage failure.
type scriptedWriter struct {
	mu    sync.Mutex
	calls int
	plan  []func(string, []byte) error
}

func (w *scriptedWriter) Writer() persist.Writer { return w.write }
func (w *scriptedWriter) write(path string, data []byte) error {
	w.mu.Lock()
	index := w.calls
	w.calls++
	var step func(string, []byte) error
	if index < len(w.plan) {
		step = w.plan[index]
	}
	w.mu.Unlock()
	if step != nil {
		return step(path, data)
	}
	return persist.AtomicWrite(path, data)
}

func testDocument() model.Document {
	return model.Document{
		Width: 8, Height: 8, Background: "#000000",
		Layers: []model.Layer{{
			ID: "plate", Kind: "rectangle", Width: 4, Height: 4, Opacity: 1,
			Transform: model.Transform{ScaleX: 1, ScaleY: 1},
			Fill:      model.Paint{Kind: "solid", Color: "#FFFFFF"},
		}},
	}
}

func newService(t *testing.T, w persist.Writer) *Service {
	t.Helper()
	store, e := persist.OpenWith(filepath.Join(t.TempDir(), "state.json"), w)
	if e != nil {
		t.Fatalf("open store: %v", e)
	}
	t.Cleanup(func() { store.Close() })
	return &Service{Store: store}
}

func prepareSnapshot(t *testing.T, s *Service) model.Snapshot {
	t.Helper()
	if _, e := s.PutScene("emblem", "emblem", 0, testDocument()); e != nil {
		t.Fatalf("put scene: %v", e)
	}
	snap, e := s.Freeze("emblem", 1)
	if e != nil {
		t.Fatalf("freeze: %v", e)
	}
	return snap
}

func TestSubmitFailureDoesNotReturnAJob(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	var mu sync.Mutex
	step := 0
	w := func(p string, d []byte) error {
		mu.Lock()
		n := step
		step++
		mu.Unlock()
		// Let scene and snapshot writes land, then reject job acceptance.
		if n == 2 {
			return &persist.WriteError{Stage: persist.StageSync, Err: errors.New("injected fsync failure")}
		}
		return persist.AtomicWrite(p, d)
	}
	store, e := persist.OpenWith(path, w)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer store.Close()
	s := &Service{Store: store}
	snap := prepareSnapshot(t, s)

	sub, e := s.SubmitDetailed(snap.ID, 1, "render-one")
	if e == nil {
		t.Fatal("expected acceptance failure")
	}
	if sub.Job.ID != "" || sub.Outcome != "" {
		t.Fatalf("failed submit leaked a result: %+v", sub)
	}
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q, want %q", got, model.NotCommitted)
	}
	// No receipt may exist, so a replay with the same key creates the job
	// exactly once once storage recovers.
	st := store.Read()
	if len(st.Receipts) != 0 {
		t.Fatalf("failed submit left a receipt: %v", st.Receipts)
	}
	job, e := s.Submit(snap.ID, 1, "render-one")
	if e != nil {
		t.Fatalf("replay after recovery: %v", e)
	}
	if job.ID == "" {
		t.Fatal("replay did not create a job")
	}
	second, e := s.Submit(snap.ID, 1, "render-one")
	if e != nil {
		t.Fatalf("second replay: %v", e)
	}
	if second.ID != job.ID {
		t.Fatalf("replay created a second job: %s != %s", second.ID, job.ID)
	}
}

func TestSubmitAmbiguousCommitIsReportedCommitted(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	var mu sync.Mutex
	step := 0
	w := func(p string, d []byte) error {
		mu.Lock()
		n := step
		step++
		mu.Unlock()
		if n == 2 {
			// Bytes are durable, but the caller sees an error, as a severed
			// response connection around a successful commit would.
			if e := persist.AtomicWrite(p, d); e != nil {
				return e
			}
			return errors.New("injected outcome loss after rename")
		}
		return persist.AtomicWrite(p, d)
	}
	store, e := persist.OpenWith(path, w)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer store.Close()
	s := &Service{Store: store}
	snap := prepareSnapshot(t, s)

	if _, e := s.Submit(snap.ID, 1, "render-one"); e == nil {
		t.Fatal("expected reported failure")
	} else if got := model.CommitmentOf(e); got != model.Committed {
		t.Fatalf("commitment = %q, want %q", got, model.Committed)
	}
	// Because the receipt is durable, the replay must resolve to the same job
	// rather than manufacturing a duplicate.
	job, e := s.Submit(snap.ID, 1, "render-one")
	if e != nil {
		t.Fatalf("replay: %v", e)
	}
	again, e := s.Submit(snap.ID, 1, "render-one")
	if e != nil {
		t.Fatalf("replay 2: %v", e)
	}
	if again.ID != job.ID {
		t.Fatalf("committed submit duplicated work: %s != %s", again.ID, job.ID)
	}
	if got := len(store.Read().Jobs); got != 1 {
		t.Fatalf("job count = %d, want 1", got)
	}
}

func TestLookupDistinguishesNeverAccepted(t *testing.T) {
	s := newService(t, persist.AtomicWrite)
	snap := prepareSnapshot(t, s)
	if _, e := s.LookupSubmission("never-used"); model.CommitmentOf(e) != model.NotCommitted {
		t.Fatalf("unknown key commitment = %v", model.CommitmentOf(e))
	} else if _, ok := model.ErrorOf(e); !ok {
		t.Fatalf("unknown key must be a domain error: %v", e)
	}
	job, e := s.Submit(snap.ID, 1, "accepted")
	if e != nil {
		t.Fatalf("submit: %v", e)
	}
	got, e := s.LookupSubmission("accepted")
	if e != nil {
		t.Fatalf("lookup accepted: %v", e)
	}
	if got.Job.ID != job.ID {
		t.Fatalf("lookup returned %s, want %s", got.Job.ID, job.ID)
	}
}

func TestOtherRequestsKeepTheirOwnOutcomeAfterFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	var mu sync.Mutex
	step := 0
	w := func(p string, d []byte) error {
		mu.Lock()
		n := step
		step++
		mu.Unlock()
		if n == 2 {
			return &persist.WriteError{Stage: persist.StageRename, Err: errors.New("injected rename failure")}
		}
		return persist.AtomicWrite(p, d)
	}
	store, e := persist.OpenWith(path, w)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	defer store.Close()
	s := &Service{Store: store}
	snap := prepareSnapshot(t, s)

	if _, e := s.Submit(snap.ID, 1, "first"); e == nil {
		t.Fatal("first request should fail")
	}
	// The next request is independent: it must be accepted with its own
	// receipt, not inherit the previous rejection.
	second, e := s.Submit(snap.ID, 1, "second")
	if e != nil {
		t.Fatalf("second request should succeed: %v", e)
	}
	if second.ID == "" {
		t.Fatal("second request has no job")
	}
	// The failed key never gained a receipt.
	if _, e := s.LookupSubmission("first"); e == nil {
		t.Fatal("rejected key appeared accepted")
	}
	third, e := s.Submit(snap.ID, 1, "second")
	if e != nil {
		t.Fatalf("replay of accepted key: %v", e)
	}
	if third.ID != second.ID {
		t.Fatalf("accepted key lost its job: %s != %s", third.ID, second.ID)
	}
}

// TestRestartDistinguishesNeverAcceptedFromCommitted reproduces the "server
// restarts after a cut response, then the client asks again" sequence.
func TestRestartDistinguishesNeverAcceptedFromCommitted(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")

	first, e := persist.Open(path)
	if e != nil {
		t.Fatalf("open: %v", e)
	}
	s := &Service{Store: first}
	snap := prepareSnapshot(t, s)
	job, e := s.Submit(snap.ID, 1, "accepted")
	if e != nil {
		t.Fatalf("submit: %v", e)
	}
	if e := first.Close(); e != nil {
		t.Fatalf("close: %v", e)
	}

	// A new process opens the same state. Neither key lookup may depend on
	// in-memory facts from the previous process.
	second, e := persist.Open(path)
	if e != nil {
		t.Fatalf("reopen: %v", e)
	}
	defer second.Close()
	restarted := &Service{Store: second}

	got, e := restarted.LookupSubmission("accepted")
	if e != nil {
		t.Fatalf("lookup accepted after restart: %v", e)
	}
	if got.Job.ID != job.ID {
		t.Fatalf("restart lost the promise: %s != %s", got.Job.ID, job.ID)
	}
	if _, e := restarted.LookupSubmission("never-sent"); e == nil {
		t.Fatal("never-accepted key resolved after restart")
	} else if model.CommitmentOf(e) != model.NotCommitted {
		t.Fatalf("unknown key commitment = %v", model.CommitmentOf(e))
	}
	// Replaying the accepted key after restart must not create a second job.
	replay, e := restarted.Submit(snap.ID, 1, "accepted")
	if e != nil {
		t.Fatalf("replay after restart: %v", e)
	}
	if replay.ID != job.ID {
		t.Fatalf("restart replay duplicated work: %s != %s", replay.ID, job.ID)
	}
	if count := len(second.Read().Jobs); count != 1 {
		t.Fatalf("job count after restart replay = %d, want 1", count)
	}
}
