package model

import (
	"errors"
	"fmt"
	"testing"
)

func TestCommitmentClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Commitment
	}{
		{"nil", nil, Committed},
		{"invalid", Invalid("bad"), NotCommitted},
		{"missing", Missing("gone"), NotCommitted},
		{"conflict", Conflict("stale"), NotCommitted},
		{"gone", Gone("purged"), NotCommitted},
		{"capacity", Busy("full"), NotCommitted},
		{"internal", Internal("disk", errors.New("boom")), NotCommitted},
		{"internal unknown", InternalWith(Unknown, "disk", errors.New("boom")), Unknown},
		{"internal committed", InternalWith(Committed, "disk", errors.New("boom")), Committed},
		{"wrapped domain", fmt.Errorf("outer: %w", Conflict("stale")), NotCommitted},
		{"raw error", errors.New("invalid JSON"), Unknown},
		{"zero commitment", &Error{Kind: KindInternal}, Unknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CommitmentOf(tc.err); got != tc.want {
				t.Fatalf("CommitmentOf = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestKindDoesNotDependOnMessage(t *testing.T) {
	// The same word in a message must not change the kind or the status
	// mapping derived from it.
	e := Internal("invalid conflict missing gone capacity", errors.New("x"))
	if kind, _ := KindOf(e); kind != KindInternal {
		t.Fatalf("kind = %q", kind)
	}
	if Code := e.(*Error).Code(); Code != "internal" {
		t.Fatalf("code = %q", Code)
	}
}
