package persist

import (
	"atlas-render/internal/model"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Writer replaces the durable state file. The default is AtomicWrite. Fault
// tests use OpenWith to interleave per-request write outcomes; production code
// always uses Open.
type Writer func(path string, data []byte) error

// WriteStage identifies how far a state replacement progressed before it
// failed. Every recognised stage leaves the previous file intact; an
// unclassified writer failure is the only ambiguous outcome.
type WriteStage string

const (
	StageCreate  WriteStage = "create"
	StageWrite   WriteStage = "write"
	StageSync    WriteStage = "sync"
	StageClose   WriteStage = "close"
	StageRename  WriteStage = "rename"
	StageUnknown WriteStage = "unknown"
)

// WriteError is the classified form of a failed state replacement. Callers
// must use its commitment rather than its message to decide whether a request
// may be replayed as new work.
type WriteError struct {
	Stage WriteStage
	Err   error
}

func (e *WriteError) Error() string { return fmt.Sprintf("write stage %s: %v", e.Stage, e.Err) }
func (e *WriteError) Unwrap() error { return e.Err }

// Commit reports whether the target file can still hold the previous state.
// Stages through rename fail before the atomic replacement, so the previous
// file is untouched.
func (e *WriteError) Commit() model.Commitment {
	if e.Stage == "" || e.Stage == StageUnknown {
		return model.Unknown
	}
	return model.NotCommitted
}

type Store struct {
	mu    sync.RWMutex
	path  string
	state model.State
	// persisted is the canonical byte image of state. An update that
	// reproduces it needs no disk write, which keeps a replay of an already
	// committed request working even while the disk is unwritable.
	persisted []byte
	// needsResync is set when a write may have landed without confirmation.
	// The next transaction must adopt the durable file before composing a new
	// state; otherwise stale memory could overwrite a newer durable fact.
	needsResync bool
	lock        *os.File
	write       Writer
}

func Open(path string) (*Store, error) { return OpenWith(path, AtomicWrite) }

// OpenWith opens a store with an explicit writer. It exists so fault tests can
// interleave write outcomes deterministically.
func OpenWith(path string, write Writer) (*Store, error) {
	if write == nil {
		return nil, errors.New("persist: writer must not be nil")
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, model.Internal("state is already opened by another process", e)
	}
	s := &Store{path: path, state: model.NewState(), lock: lock, write: write}
	data, e := os.ReadFile(path)
	if e != nil && !os.IsNotExist(e) {
		s.Close()
		return nil, e
	}
	if e == nil {
		if e = json.Unmarshal(data, &s.state); e != nil {
			s.Close()
			return nil, model.Internal("invalid persisted JSON", e)
		}
		if e = Validate(s.state); e != nil {
			s.Close()
			return nil, model.Internal("persisted state failed validation", e)
		}
		// Use a canonical image so a transaction that reproduces the loaded
		// state is recognised as a no-op even if the file was reformatted.
		if canonical, me := json.Marshal(s.state); me == nil {
			s.persisted = canonical
		} else {
			s.persisted = data
		}
	}
	return s, nil
}
func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	e := s.lock.Close()
	s.lock = nil
	return e
}
func (s *Store) Read() model.State { s.mu.RLock(); defer s.mu.RUnlock(); return model.Clone(s.state) }

// Update applies fn to a copy of the state and makes the result durable before
// publishing it in memory.
//
// Errors returned by fn are domain rejections and pass through unchanged.
// Validation, encoding and write failures are internal: the caller's input is
// not to blame, and the state file was not replaced unless the writer reports
// an unknown outcome. No error path advances the in-memory state, so a caller
// that receives an error never observes a phantom commit.
func (s *Store) Update(fn func(*model.State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.needsResync {
		// A previous write outcome was ambiguous. Adopt the durable file
		// before composing anything new, so this transaction builds on the
		// fact that actually landed rather than on stale memory. If the file
		// cannot be read or validated, fail as Unknown: the caller must not
		// treat this as a safe replay of a fresh operation.
		if e := s.reloadLocked(); e != nil {
			return model.InternalWith(model.Unknown, "state could not be re-read after an unresolved write", e)
		}
	}
	next := model.Clone(s.state)
	if e := fn(&next); e != nil {
		if _, ok := model.ErrorOf(e); ok {
			return e
		}
		// fn runs before any write, so an unclassified callback failure is
		// still a definite non-commit.
		return model.Internal("state transition failed", e)
	}
	if e := Validate(next); e != nil {
		return model.Internal("resulting state failed integrity validation", e)
	}
	b, e := json.Marshal(next)
	if e != nil {
		return model.Internal("state could not be encoded", e)
	}
	if bytes.Equal(s.persisted, b) {
		// The transaction changed nothing. Replaying a committed request must
		// not depend on the disk being writable.
		s.state = next
		return nil
	}
	if e = s.write(s.path, b); e != nil {
		commitment := writeCommitment(e)
		if commitment != model.Unknown {
			return model.InternalWith(commitment, "state could not be persisted", e)
		}
		// The writer could not prove the target file is unchanged. Resolve the
		// ambiguity against the durable file now, so this request receives the
		// narrowest truthful commitment and later requests still get theirs.
		verdict, verifyErr := s.verifyLocked(b)
		if verifyErr != nil {
			// Memory may now disagree with the durable file. The next
			// transaction must re-read the file before composing anything, or
			// it could overwrite a committed fact with stale data.
			s.needsResync = true
			return model.InternalWith(model.Unknown, "state write outcome could not be verified", verifyErr)
		}
		return model.InternalWith(verdict, "state could not be persisted", e)
	}
	s.state = next
	s.persisted = b
	return nil
}

// verifyLocked inspects the durable file after an ambiguous writer failure and
// returns the commitment it can prove. The caller holds the write lock.
//
// If the file now holds the attempted image, the write landed. If it still
// validates as a complete state but holds something else, the attempted write
// did not land and memory is refreshed from the durable truth. Any other
// outcome stays Unknown and the store must be reopened before accepting work.
func (s *Store) verifyLocked(attempted []byte) (model.Commitment, error) {
	data, e := os.ReadFile(s.path)
	if e != nil {
		if os.IsNotExist(e) {
			// No state file at all: the first successful write never landed,
			// so the attempted transition is definitely absent.
			return model.NotCommitted, nil
		}
		return model.Unknown, e
	}
	if bytes.Equal(data, attempted) {
		// The attempt is durable; publish it so memory matches the file.
		var committed model.State
		if e = json.Unmarshal(data, &committed); e != nil {
			return model.Unknown, e
		}
		if e = Validate(committed); e != nil {
			// Bytes match but do not form a valid state: do not claim a
			// commit that callers could not later read.
			return model.Unknown, e
		}
		s.state = committed
		s.persisted = attempted
		s.needsResync = false
		return model.Committed, nil
	}
	var current model.State
	if e = json.Unmarshal(data, &current); e != nil {
		return model.Unknown, e
	}
	if e = Validate(current); e != nil {
		return model.Unknown, e
	}
	// A complete, different image is the previous durable state: this attempt
	// did not land, and any later transaction must build on that image.
	s.state = current
	if canonical, me := json.Marshal(current); me == nil {
		s.persisted = canonical
	} else {
		s.persisted = data
	}
	s.needsResync = false
	return model.NotCommitted, nil
}

// reloadLocked re-reads the durable file into memory. The caller holds the
// write lock. A missing file means the last replacement never landed, so the
// previous in-memory state is still the authoritative one.
func (s *Store) reloadLocked() error {
	data, e := os.ReadFile(s.path)
	if e != nil {
		if os.IsNotExist(e) {
			s.needsResync = false
			return nil
		}
		return e
	}
	var loaded model.State
	if e = json.Unmarshal(data, &loaded); e != nil {
		return e
	}
	if e = Validate(loaded); e != nil {
		return e
	}
	s.state = loaded
	if canonical, me := json.Marshal(loaded); me == nil {
		s.persisted = canonical
	} else {
		s.persisted = data
	}
	s.needsResync = false
	return nil
}

func writeCommitment(err error) model.Commitment {
	var we *WriteError
	if errors.As(err, &we) {
		return we.Commit()
	}
	// An unrecognised writer failure is not proof that the target file is
	// unchanged, so it must not be reported as safe to resubmit.
	return model.Unknown
}

// AtomicWrite replaces path only after the complete new image is durable in a
// temporary file in the same directory.
func AtomicWrite(path string, data []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".atlas-write-")
	if e != nil {
		return &WriteError{Stage: StageCreate, Err: e}
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e != nil {
		f.Close()
		stage := StageWrite
		if pe, ok := e.(*os.PathError); ok && pe.Op == "chmod" {
			stage = StageCreate
		}
		return &WriteError{Stage: stage, Err: e}
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return &WriteError{Stage: StageSync, Err: e}
	}
	if e = f.Close(); e != nil {
		return &WriteError{Stage: StageClose, Err: e}
	}
	if e = os.Rename(name, path); e != nil {
		return &WriteError{Stage: StageRename, Err: e}
	}
	return nil
}
