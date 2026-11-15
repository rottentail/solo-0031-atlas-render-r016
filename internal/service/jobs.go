package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/raster"
	"atlas-render/internal/scene"
	"fmt"
	"time"
)

// SubmissionOutcome describes what a Submit call established.
type SubmissionOutcome string

const (
	// SubmissionCreated means this call durably created the job.
	SubmissionCreated SubmissionOutcome = "created"
	// SubmissionReplayed means the idempotency key already pointed at a job;
	// no new work was created and the original job is returned.
	SubmissionReplayed SubmissionOutcome = "replayed"
)

// Submission is the complete result of a submit attempt.
//
// Job is populated only when the durable outcome is committed. On error the
// zero value is returned, so a direct caller cannot mistake a rejected or
// uncommitted attempt for an accepted task.
type Submission struct {
	Job     model.Job
	Outcome SubmissionOutcome
}

// Submit accepts a render task under an idempotency key and returns the job
// that the key identifies.
//
// The receipt and the job are written in a single transaction. On error the
// zero job is returned. Callers that need to distinguish a fresh acceptance
// from an idempotent replay use SubmitDetailed; callers deciding whether they
// may resubmit must consult model.CommitmentOf(err), never the message:
//   - NotCommitted: no receipt exists; replaying the same key is safe.
//   - Committed: the receipt exists; replaying the same key returns it.
//   - Unknown: the outcome cannot be proven; replay the same key or read the
//     service state after restart. Never resubmit under a different key.
func (s *Service) Submit(snapshotID string, scale int, key string) (model.Job, error) {
	v, e := s.SubmitDetailed(snapshotID, scale, key)
	if e != nil {
		return model.Job{}, e
	}
	return v.Job, nil
}

// SubmitDetailed is Submit with the outcome recorded explicitly.
func (s *Service) SubmitDetailed(snapshotID string, scale int, key string) (Submission, error) {
	var out Submission
	if !scene.ValidID(key) {
		return Submission{}, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	fingerprint := model.CanonicalDigest([]any{snapshotID, scale})
	e := s.Store.Update(func(st *model.State) error {
		if r, ok := st.Receipts[key]; ok {
			if r.Fingerprint != fingerprint {
				return model.Conflict("idempotency key used for different request")
			}
			j, ok := st.Jobs[r.JobID]
			if !ok {
				// The receipt is the promise; a missing job means the durable
				// facts disagree. The key is already taken, so the outcome is
				// not "safe to submit again": report it as unknown.
				return model.InternalWith(model.Unknown, "idempotency receipt has no job", nil)
			}
			out = Submission{Job: j, Outcome: SubmissionReplayed}
			return nil
		}
		snap, ok := st.Snapshots[snapshotID]
		if !ok {
			return model.Missing("snapshot not found")
		}
		if snap.State != "active" {
			return model.Conflict("snapshot is revoked")
		}
		if _, e := raster.Compile(snap, scale); e != nil {
			return e
		}
		pending := 0
		for _, j := range st.Jobs {
			if j.State == "queued" || j.State == "running" {
				pending++
			}
		}
		if pending >= 64 {
			return model.Busy("render queue is full")
		}
		now := time.Now().UTC()
		job := model.Job{ID: model.ID("job", st.Sequence+1), SnapshotID: snapshotID, State: "queued", Scale: scale, Attempt: 1, CreatedAt: now, UpdatedAt: now, Payload: snap}
		st.Jobs[job.ID] = job
		st.Receipts[key] = model.Receipt{Key: key, Fingerprint: fingerprint, JobID: job.ID}
		st.Emit("job.queued", job.ID)
		out = Submission{Job: job, Outcome: SubmissionCreated}
		return nil
	})
	if e != nil {
		// Never leak a partially built job alongside a failed commit.
		return Submission{}, e
	}
	// The fixed payload is not part of this response path.
	out.Job.Payload = model.Snapshot{}
	return out, nil
}

// LookupSubmission resolves an idempotency key without creating work. Direct
// callers use this to distinguish "never accepted" (Missing) from "already
// promised", including after a process restart.
func (s *Service) LookupSubmission(key string) (Submission, error) {
	if !scene.ValidID(key) {
		return Submission{}, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	st := s.Store.Read()
	r, ok := st.Receipts[key]
	if !ok {
		return Submission{}, model.Missing("idempotency key was never accepted")
	}
	j, ok := st.Jobs[r.JobID]
	if !ok {
		return Submission{}, model.InternalWith(model.Unknown, "idempotency receipt has no job", nil)
	}
	j.Payload = model.Snapshot{}
	return Submission{Job: j, Outcome: SubmissionReplayed}, nil
}

func (s *Service) Cancel(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State == "cancelled" {
			out = j
			return nil
		}
		if j.State != "queued" && j.State != "running" {
			return model.Conflict("job has already finished")
		}
		j.State = "cancelled"
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.cancelled", id)
		out = j
		return nil
	})
	if e != nil {
		return model.Job{}, e
	}
	return out, nil
}
func (s *Service) Retry(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State != "failed" && j.State != "cancelled" {
			return model.Conflict("only failed or cancelled jobs can retry")
		}
		n := 0
		for _, v := range st.Jobs {
			if v.State == "queued" || v.State == "running" {
				n++
			}
		}
		if n >= 64 {
			return model.Busy("render queue is full")
		}
		j.State = "queued"
		j.Error = ""
		j.Attempt++
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.retried", id)
		out = j
		return nil
	})
	if e != nil {
		return model.Job{}, e
	}
	return out, nil
}
func (s *Service) Purge(id string) error {
	return s.Store.Update(func(st *model.State) error {
		a, ok := st.Artifacts[id]
		if !ok {
			return model.Missing("artifact not found")
		}
		if a.State == "purged" {
			return nil
		}
		a.State = "purged"
		a.PNG = nil
		st.Artifacts[id] = a
		st.Emit("artifact.purged", id)
		return nil
	})
}
func (s *Service) Artifact(id string) (model.Artifact, error) {
	a, ok := s.Store.Read().Artifacts[id]
	if !ok {
		return a, model.Missing("artifact not found")
	}
	if a.State == "purged" {
		return a, model.Gone(fmt.Sprintf("artifact %s was purged", id))
	}
	return a, nil
}
