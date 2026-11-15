package persist

import (
	"atlas-render/internal/model"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// faultWriter lets a test decide, per call, whether a replacement lands, fails
// before the rename, or fails in a way that leaves the outcome ambiguous.
type faultWriter struct {
	mu    sync.Mutex
	calls int
	plan  []func(path string, data []byte) error
}

func (w *faultWriter) Writer() Writer { return w.write }

func (w *faultWriter) write(path string, data []byte) error {
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
	return AtomicWrite(path, data)
}

// land applies the bytes exactly as the production writer would.
func land(path string, data []byte) error { return AtomicWrite(path, data) }

// failBeforeRename simulates an injected filesystem failure with a recognised
// stage: the previous file must remain intact.
func failBeforeRename(path string, data []byte) error {
	return &WriteError{Stage: StageSync, Err: errors.New("injected fsync failure")}
}

// ambiguousWrite performs the real replacement and then reports failure, as an
// interrupted writer or post-rename error would. The bytes are durable even
// though the caller saw an error.
func ambiguousWrite(path string, data []byte) error {
	if e := AtomicWrite(path, data); e != nil {
		return e
	}
	return errors.New("injected outcome loss after rename")
}

func openStore(t *testing.T, path string, w Writer) *Store {
	t.Helper()
	s, e := OpenWith(path, w)
	if e != nil {
		t.Fatalf("open store: %v", e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seedScene(s *Store) error {
	return s.Update(func(st *model.State) error {
		st.Scenes["base"] = validScene("base")
		return nil
	})
}

// validScene builds a minimal scene that passes state validation, so the tests
// exercise storage outcomes rather than document validation.
func validScene(id string) model.Scene {
	return model.Scene{
		ID:       id,
		Name:     id,
		Revision: 1,
		State:    "active",
		Document: model.Document{
			Width: 2, Height: 2, Background: "#000000",
			Layers: []model.Layer{{
				ID: "plate", Kind: "rectangle", Width: 1, Height: 1, Opacity: 1,
				Transform: model.Transform{ScaleX: 1, ScaleY: 1},
				Fill:      model.Paint{Kind: "solid", Color: "#FFFFFF"},
			}},
		},
	}
}

func TestUpdateFailureLeavesStateReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	w := &faultWriter{plan: []func(string, []byte) error{land, failBeforeRename}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	before := s.Read()
	e := s.Update(func(st *model.State) error {
		st.Scenes["second"] = validScene("second")
		return nil
	})
	if e == nil {
		t.Fatal("expected injected write failure")
	}
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q, want %q", got, model.NotCommitted)
	}
	if _, ok := model.ErrorOf(e); !ok {
		t.Fatalf("write failure must be a classified internal error: %v", e)
	}
	after := s.Read()
	if len(after.Scenes) != len(before.Scenes) {
		t.Fatalf("in-memory state advanced on failed write: %v", after.Scenes)
	}
	if _, ok := after.Scenes["second"]; ok {
		t.Fatal("failed transaction leaked into memory")
	}
	// The durable file must also still hold the old fact.
	reopened := openAfterClose(t, s, path)
	if _, ok := reopened.Read().Scenes["second"]; ok {
		t.Fatal("failed transaction reached the durable file")
	}
}

func TestDomainRejectionIsNotCommitted(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, filepath.Join(dir, "state.json"), AtomicWrite)
	e := s.Update(func(st *model.State) error { return model.Conflict("nope") })
	if e == nil || model.CommitmentOf(e) != model.NotCommitted {
		t.Fatalf("domain rejection commitment = %v (%v)", model.CommitmentOf(e), e)
	}
	if kind, ok := model.KindOf(e); !ok || kind != model.KindConflict {
		t.Fatalf("domain rejection kind = %v", e)
	}
}

func TestAmbiguousWriteIsVerifiedAgainstDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	w := &faultWriter{plan: []func(string, []byte) error{land, ambiguousWrite}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	e := s.Update(func(st *model.State) error {
		st.Scenes["committed"] = validScene("committed")
		return nil
	})
	if e == nil {
		t.Fatal("expected reported failure")
	}
	// The bytes landed, so the only truthful answer is Committed; reporting
	// NotCommitted here is what would let a client duplicate work.
	if got := model.CommitmentOf(e); got != model.Committed {
		t.Fatalf("commitment = %q, want %q", got, model.Committed)
	}
	if _, ok := s.Read().Scenes["committed"]; !ok {
		t.Fatal("verified commit was not published to memory")
	}
}

func TestAmbiguousUnlandedWriteIsNotCommitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	w := &faultWriter{plan: []func(string, []byte) error{land, func(p string, d []byte) error {
		return errors.New("injected opaque failure, bytes not written")
	}}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	e := s.Update(func(st *model.State) error {
		st.Scenes["lost"] = validScene("lost")
		return nil
	})
	if e == nil {
		t.Fatal("expected reported failure")
	}
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q, want %q", got, model.NotCommitted)
	}
	if _, ok := s.Read().Scenes["lost"]; ok {
		t.Fatal("unlanded write published to memory")
	}
}

func TestConcurrentRequestsKeepTheirOwnOutcome(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// First write lands, second is rejected before rename, third lands.
	var mu sync.Mutex
	steps := 0
	w := func(p string, d []byte) error {
		mu.Lock()
		index := steps
		steps++
		mu.Unlock()
		if index == 1 {
			return &WriteError{Stage: StageRename, Err: errors.New("injected rename failure")}
		}
		return AtomicWrite(p, d)
	}
	s := openStore(t, path, w)
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}

	// Serialise the two requests through the store: request A is rejected,
	// request B must still succeed and get its own answer.
	e := s.Update(func(st *model.State) error {
		st.Scenes["a"] = validScene("a")
		return nil
	})
	if e == nil || model.CommitmentOf(e) != model.NotCommitted {
		t.Fatalf("request A: commitment = %v err = %v", model.CommitmentOf(e), e)
	}
	e = s.Update(func(st *model.State) error {
		st.Scenes["b"] = validScene("b")
		return nil
	})
	if e != nil {
		t.Fatalf("request B should succeed after A's failure: %v", e)
	}
	st := s.Read()
	if _, ok := st.Scenes["b"]; !ok {
		t.Fatal("request B's fact missing")
	}
	if _, ok := st.Scenes["a"]; ok {
		t.Fatal("request A's rejected fact appeared")
	}
}

func TestReplayOfCommittedStateNeedsNoWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	w := &faultWriter{plan: []func(string, []byte) error{land, func(string, []byte) error {
		return &WriteError{Stage: StageCreate, Err: errors.New("disk unwritable")}
	}}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	// An update that changes nothing must not consult the writer at all, so a
	// replay remains observable through the service even when the disk is down.
	e := s.Update(func(st *model.State) error { return nil })
	if e != nil {
		t.Fatalf("no-op update should not write: %v", e)
	}
	if got := model.CommitmentOf(e); got != model.Committed {
		t.Fatalf("no-op commitment = %q", got)
	}
}

func TestUnverifiableWriteStaysUnknownAndBlocksStaleOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// First write lands normally; the second destroys the durable file and
	// reports an opaque failure, so neither outcome can be proven.
	w := &faultWriter{plan: []func(string, []byte) error{land, func(p string, d []byte) error {
		if e := os.Remove(p); e != nil {
			return e
		}
		return errors.New("opaque failure after losing the file")
	}}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	e := s.Update(func(st *model.State) error {
		st.Scenes["x"] = validScene("x")
		return nil
	})
	if e == nil {
		t.Fatal("expected reported failure")
	}
	// Missing file means the attempt definitely did not land.
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q, want %q", got, model.NotCommitted)
	}
	if _, ok := s.Read().Scenes["x"]; ok {
		t.Fatal("unlanded write published to memory")
	}
	if got := model.CommitmentOf(errors.New("opaque")); got != model.Unknown {
		t.Fatalf("unclassified error commitment = %q, want %q", got, model.Unknown)
	}
}

func openAfterClose(t *testing.T, s *Store, path string) *Store {
	t.Helper()
	if e := s.Close(); e != nil {
		t.Fatalf("close: %v", e)
	}
	re, e := Open(path)
	if e != nil {
		t.Fatalf("reopen: %v", e)
	}
	t.Cleanup(func() { re.Close() })
	return re
}

func TestUnverifiableWriteForcesResyncBeforeNextTransaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// The second write corrupts the durable file and reports an opaque
	// failure, so neither the attempt nor the previous state can be read back.
	w := &faultWriter{plan: []func(string, []byte) error{land, func(p string, d []byte) error {
		if e := os.WriteFile(p, []byte("{not json"), 0600); e != nil {
			return e
		}
		return errors.New("opaque failure after corrupting the file")
	}}}
	s := openStore(t, path, w.Writer())
	if e := seedScene(s); e != nil {
		t.Fatalf("seed: %v", e)
	}
	e := s.Update(func(st *model.State) error {
		st.Scenes["x"] = validScene("x")
		return nil
	})
	if e == nil {
		t.Fatal("expected reported failure")
	}
	if got := model.CommitmentOf(e); got != model.Unknown {
		t.Fatalf("commitment = %q, want %q", got, model.Unknown)
	}
	// The next transaction must not compose on top of memory that may now
	// disagree with the file. It re-reads, finds it unusable, and refuses.
	e = s.Update(func(st *model.State) error {
		st.Scenes["y"] = validScene("y")
		return nil
	})
	if e == nil {
		t.Fatal("expected refusal after an unverifiable write")
	}
	if got := model.CommitmentOf(e); got != model.Unknown {
		t.Fatalf("follow-up commitment = %q, want %q", got, model.Unknown)
	}
	if _, ok := s.Read().Scenes["y"]; ok {
		t.Fatal("stale overwrite was published")
	}
}
