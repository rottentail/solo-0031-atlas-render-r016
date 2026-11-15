package model

import (
	"errors"
	"fmt"
)

// Kind is the stable domain classification of an error.
//
// It deliberately does not carry an HTTP status code. Transports decide how a
// kind is rendered; domain code and direct service callers reason only about
// the kind.
type Kind string

const (
	KindInvalid  Kind = "invalid"
	KindMissing  Kind = "missing"
	KindConflict Kind = "conflict"
	KindGone     Kind = "gone"
	KindCapacity Kind = "capacity"
	KindInternal Kind = "internal"
)

// Commitment states whether a requested state transition became a durable
// fact. It is the only input callers may use when deciding whether an
// operation may be retried. Error text is never part of that decision.
type Commitment string

const (
	// NotCommitted means the durable state was not replaced. Replaying the
	// same request cannot duplicate work.
	NotCommitted Commitment = "not-committed"
	// Committed means the transition is durable. The same request must be
	// replayed with the same identity (idempotency key) rather than submitted
	// as a new operation.
	Committed Commitment = "committed"
	// Unknown means neither outcome can be proven, for example a response that
	// was cut after the server may have committed. Callers must replay the
	// same request with the same identity or read the target; they must not
	// assume a new submission is safe.
	Unknown Commitment = "unknown"
)

// Error is a classified domain or transaction error. Message is for humans and
// logs only; Kind and Commitment are the machine-readable contract.
type Error struct {
	Kind       Kind
	Commitment Commitment
	Message    string
	cause      error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return string(e.Kind)
}
func (e *Error) Unwrap() error { return e.cause }

// Code is the stable code exposed by transports and persisted logs.
func (e *Error) Code() string { return string(e.Kind) }

// Commit reports the durable outcome. A zero commitment is not evidence of
// nothing being written, so it is reported as Unknown rather than
// NotCommitted.
func (e *Error) Commit() Commitment {
	if e.Commitment == "" {
		return Unknown
	}
	return e.Commitment
}

// Domain rejections happen before any durable write, so they are always
// NotCommitted. Internal errors also default to NotCommitted; persistence code
// overrides that with InternalWith when it knows more.
func Invalid(format string, args ...any) error {
	return &Error{Kind: KindInvalid, Commitment: NotCommitted, Message: fmt.Sprintf(format, args...)}
}
func Conflict(message string) error {
	return &Error{Kind: KindConflict, Commitment: NotCommitted, Message: message}
}
func Missing(message string) error {
	return &Error{Kind: KindMissing, Commitment: NotCommitted, Message: message}
}
func Gone(message string) error {
	return &Error{Kind: KindGone, Commitment: NotCommitted, Message: message}
}
func Busy(message string) error {
	return &Error{Kind: KindCapacity, Commitment: NotCommitted, Message: message}
}

// Internal reports an internal failure that definitely did not change durable
// state.
func Internal(message string, cause error) error {
	return &Error{Kind: KindInternal, Commitment: NotCommitted, Message: message, cause: cause}
}

// InternalWith reports an internal failure with an explicit commitment.
func InternalWith(commitment Commitment, message string, cause error) error {
	if commitment == "" {
		commitment = Unknown
	}
	return &Error{Kind: KindInternal, Commitment: commitment, Message: message, cause: cause}
}

// ErrorOf extracts the classified error without inspecting its message.
func ErrorOf(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// KindOf reports the domain kind of err, if it has one.
func KindOf(err error) (Kind, bool) {
	if e, ok := ErrorOf(err); ok {
		return e.Kind, true
	}
	return "", false
}

// CommitmentOf is the retry-safety contract for every caller, local or remote.
//
// Unclassified errors are deliberately reported as Unknown rather than
// NotCommitted: a raw low-level error is not evidence that no state was
// written, and treating it as safe to resubmit is how duplicate work is
// created.
func CommitmentOf(err error) Commitment {
	if err == nil {
		return Committed
	}
	if e, ok := ErrorOf(err); ok {
		return e.Commit()
	}
	return Unknown
}
