package engine

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/raster"
	"context"
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

type Worker struct {
	store  *persist.Store
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// renderResult is the compressed outcome of one attempt. It is kept separate
// from the transaction that records it: rendering can fail for domain reasons
// or environment reasons, and only the commit itself decides the job state.
type renderResult struct {
	err       error
	cancelled bool
	artifact  *model.Artifact
}

func New(st *persist.Store) *Worker {
	return &Worker{store: st, active: map[string]context.CancelFunc{}}
}

// Recover returns running jobs to the queue without incrementing the attempt.
// A failed recovery write is an internal error: callers must not treat the
// service as ready, because the durable queue is still in its old shape.
func (w *Worker) Recover() error {
	return w.store.Update(func(s *model.State) error {
		for id, j := range s.Jobs {
			if j.State == "running" {
				j.State = "queued"
				j.UpdatedAt = time.Now().UTC()
				s.Jobs[id] = j
				s.Emit("job.recovered", id)
			}
		}
		return nil
	})
}
func (w *Worker) Cancel(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cancel := w.active[id]; cancel != nil {
		cancel()
	}
}
func (w *Worker) Run(ctx context.Context) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if e := w.step(ctx); e != nil {
				log.Printf("render worker: %v", e)
			}
		}
	}
}
func (w *Worker) claim() (model.Job, bool, error) {
	var job model.Job
	found := false
	state := w.store.Read()
	ids := []string{}
	for id, j := range state.Jobs {
		if j.State == "queued" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return job, false, nil
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := state.Jobs[ids[i]], state.Jobs[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return a.ID < b.ID
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	e := w.store.Update(func(s *model.State) error {
		j := s.Jobs[ids[0]]
		if j.State != "queued" {
			return nil
		}
		j.State = "running"
		j.UpdatedAt = time.Now().UTC()
		s.Jobs[j.ID] = j
		s.Emit("job.running", j.ID)
		job = j
		found = true
		return nil
	})
	if e != nil {
		// A failed claim write means the job is still queued durably; the
		// worker did not acquire it and must not render it.
		return model.Job{}, false, e
	}
	return job, found, nil
}
func (w *Worker) step(parent context.Context) error {
	j, ok, e := w.claim()
	if e != nil || !ok {
		return e
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	w.mu.Lock()
	w.active[j.ID] = cancel
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.active, j.ID); w.mu.Unlock() }()
	// Cancellation can commit between claim and tracking; consult the committed state.
	current := w.store.Read().Jobs[j.ID]
	if current.State != "running" {
		cancel()
	}
	result := w.render(ctx, j)
	if parent.Err() != nil {
		// The process is stopping. Leave the job running; startup recovery
		// will requeue it without treating this attempt as failed.
		return nil
	}
	return w.record(j, result)
}

// render performs one attempt and classifies its outcome without touching
// durable state.
func (w *Worker) render(ctx context.Context, j model.Job) renderResult {
	plan, e := raster.Compile(j.Payload, j.Scale)
	if e != nil {
		return renderResult{err: e}
	}
	img, e := plan.Render(ctx)
	if e != nil {
		return renderResult{err: e, cancelled: errors.Is(e, context.Canceled)}
	}
	data, e := codec.Encode(img)
	if e != nil {
		return renderResult{err: e}
	}
	a := model.Artifact{ID: "", JobID: j.ID, State: "live", Digest: model.Digest(data), Width: plan.Width, Height: plan.Height, PNG: data}
	return renderResult{artifact: &a}
}

// record commits an attempt's outcome.
//
// The job identity (state and attempt) is re-checked inside the transaction, so
// a cancel or retry that committed during rendering wins and a stale attempt
// records nothing. A successful-looking render is turned into a succeeded job
// only if the same transaction also stores its artifact; the two facts become
// visible together or not at all.
func (w *Worker) record(attempt model.Job, result renderResult) error {
	return w.store.Update(func(s *model.State) error {
		v, ok := s.Jobs[attempt.ID]
		if !ok {
			return model.Internal("claimed job disappeared", nil)
		}
		if v.State != "running" || v.Attempt != attempt.Attempt {
			// A newer fact (cancel or retry) already decided this attempt.
			return nil
		}
		v.UpdatedAt = time.Now().UTC()
		if result.cancelled || result.err != nil {
			if result.cancelled {
				v.State = "cancelled"
			} else {
				v.State = "failed"
				v.Error = result.err.Error()
			}
			s.Jobs[v.ID] = v
			s.Emit("job."+v.State, v.ID)
			return nil
		}
		if result.artifact == nil {
			// A nil artifact without an error is a programming fault, not a
			// legitimate success; failing the attempt avoids a phantom job.
			v.State = "failed"
			v.Error = "render produced no artifact"
			s.Jobs[v.ID] = v
			s.Emit("job.failed", v.ID)
			return nil
		}
		a := *result.artifact
		a.ID = model.ID("png", s.Sequence+1)
		a.JobID = v.ID
		s.Artifacts[a.ID] = a
		v.State = "succeeded"
		v.ArtifactID = a.ID
		s.Jobs[v.ID] = v
		s.Emit("job.succeeded", v.ID)
		return nil
	})
}
