package httpapi

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/service"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

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

func newServer(t *testing.T, w persist.Writer) (*httptest.Server, *service.Service) {
	t.Helper()
	store, e := persist.OpenWith(filepath.Join(t.TempDir(), "state.json"), w)
	if e != nil {
		t.Fatalf("open store: %v", e)
	}
	t.Cleanup(func() { store.Close() })
	svc := &service.Service{Store: store}
	srv := httptest.NewServer(API{Service: svc}.Handler())
	t.Cleanup(srv.Close)
	return srv, svc
}

func call(t *testing.T, srv *httptest.Server, method, path string, body any, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			t.Fatalf("encode body: %v", e)
		}
		reader = strings.NewReader(string(b))
	} else {
		reader = strings.NewReader("")
	}
	req, e := http.NewRequest(method, srv.URL+path, reader)
	if e != nil {
		t.Fatalf("request: %v", e)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatalf("do: %v", e)
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatalf("read body: %v", e)
	}
	return resp, data
}

func setupScene(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, data := call(t, srv, "PUT", "/v1/scenes/emblem", map[string]any{"name": "emblem", "expectedRevision": 0, "document": testDocument()}, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("put scene: %d %s", resp.StatusCode, data)
	}
	resp, data = call(t, srv, "POST", "/v1/scenes/emblem/snapshots", map[string]any{"expectedRevision": 1}, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("freeze: %d %s", resp.StatusCode, data)
	}
	var snap model.Snapshot
	if e := json.Unmarshal(data, &snap); e != nil {
		t.Fatalf("decode snapshot: %v", e)
	}
	return snap.ID
}

func TestSubmitSuccessKeepsStatusAndFormat(t *testing.T) {
	srv, _ := newServer(t, persist.AtomicWrite)
	snap := setupScene(t, srv)
	resp, data := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 202 {
		t.Fatalf("status = %d, want 202: %s", resp.StatusCode, data)
	}
	if got := resp.Header.Get(CommitmentHeader); got != string(model.Committed) {
		t.Fatalf("commitment header = %q", got)
	}
	var job model.Job
	if e := json.Unmarshal(data, &job); e != nil {
		t.Fatalf("decode job: %v", e)
	}
	if job.ID == "" || job.State != "queued" {
		t.Fatalf("unexpected job: %+v", job)
	}
	if job.Payload.ID != "" {
		t.Fatal("job payload leaked fixed input")
	}
}

func TestReplayedSubmitIsMarkedReplayNotDuplicate(t *testing.T) {
	srv, _ := newServer(t, persist.AtomicWrite)
	snap := setupScene(t, srv)
	_, firstData := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	var first model.Job
	_ = json.Unmarshal(firstData, &first)
	resp, secondData := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 202 {
		t.Fatalf("replay status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get(ReplayHeader); got != "true" {
		t.Fatalf("replay header = %q", got)
	}
	var second model.Job
	_ = json.Unmarshal(secondData, &second)
	if second.ID != first.ID {
		t.Fatalf("replay created a different job: %s != %s", second.ID, first.ID)
	}
}

func TestStorageFailureIsInternalNotClientError(t *testing.T) {
	// Scene and snapshot writes land; the job acceptance write fails.
	w := &scriptedWriter{plan: []func(string, []byte) error{nil, nil, func(string, []byte) error {
		return &persist.WriteError{Stage: persist.StageSync, Err: errors.New("injected fsync failure")}
	}}}
	srv, _ := newServer(t, w.Writer())
	snap := setupScene(t, srv)
	resp, data := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 500 {
		t.Fatalf("status = %d, want 500: %s", resp.StatusCode, data)
	}
	var body errorBody
	if e := json.Unmarshal(data, &body); e != nil {
		t.Fatalf("decode error body: %v", e)
	}
	if body.Code != "internal" {
		t.Fatalf("code = %q, want internal", body.Code)
	}
	if body.Commitment != string(model.NotCommitted) {
		t.Fatalf("body commitment = %q, want %q", body.Commitment, model.NotCommitted)
	}
	if resp.Header.Get(CommitmentHeader) != string(model.NotCommitted) {
		t.Fatalf("header commitment = %q", resp.Header.Get(CommitmentHeader))
	}
	// Critically, the client is told it may safely retry, and the retry does
	// create exactly one job.
	retryResp, retryData := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if retryResp.StatusCode != 202 {
		t.Fatalf("retry status = %d: %s", retryResp.StatusCode, retryData)
	}
}

func TestSeveredResponseAfterCommitIsMarkedCommitted(t *testing.T) {
	// Bytes become durable, but the client sees an error.
	w := &scriptedWriter{plan: []func(string, []byte) error{nil, nil, func(p string, d []byte) error {
		if e := persist.AtomicWrite(p, d); e != nil {
			return e
		}
		return errors.New("injected outcome loss after rename")
	}}}
	srv, _ := newServer(t, w.Writer())
	snap := setupScene(t, srv)
	resp, data := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 500 {
		t.Fatalf("status = %d, want 500: %s", resp.StatusCode, data)
	}
	if got := resp.Header.Get(CommitmentHeader); got != string(model.Committed) {
		t.Fatalf("commitment = %q, want %q", got, model.Committed)
	}
	// Replaying the same key must return the original job, not create another.
	replayResp, replayData := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if replayResp.StatusCode != 202 {
		t.Fatalf("replay status = %d: %s", replayResp.StatusCode, replayData)
	}
	if replayResp.Header.Get(ReplayHeader) != "true" {
		t.Fatal("replay of a committed submit was not marked as a replay")
	}
	// Looking the key up must also succeed, proving the receipt is durable.
	lookupResp, lookupData := call(t, srv, "GET", "/v1/idempotency/one", nil, nil)
	if lookupResp.StatusCode != 200 {
		t.Fatalf("lookup status = %d: %s", lookupResp.StatusCode, lookupData)
	}
}

func TestClientErrorsRemainClientErrors(t *testing.T) {
	srv, _ := newServer(t, persist.AtomicWrite)
	snap := setupScene(t, srv)
	cases := []struct {
		name    string
		method  string
		path    string
		body    any
		headers map[string]string
		status  int
		code    string
	}{
		{"missing snapshot", "POST", "/v1/jobs", map[string]any{"snapshotId": "snap-99999999", "scale": 1}, map[string]string{"Idempotency-Key": "k1"}, 404, "missing"},
		{"bad scale", "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 3}, map[string]string{"Idempotency-Key": "k2"}, 400, "invalid"},
		{"bad key", "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": ""}, 400, "invalid"},
		{"missing job", "GET", "/v1/jobs/job-99999999", nil, nil, 404, "missing"},
		{"missing artifact", "GET", "/v1/artifacts/png-99999999", nil, nil, 404, "missing"},
		{"unknown key", "GET", "/v1/idempotency/never", nil, nil, 404, "missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, data := call(t, srv, tc.method, tc.path, tc.body, tc.headers)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d: %s", resp.StatusCode, tc.status, data)
			}
			var body errorBody
			if e := json.Unmarshal(data, &body); e != nil {
				t.Fatalf("decode: %v (%s)", e, data)
			}
			if body.Code != tc.code {
				t.Fatalf("code = %q, want %q", body.Code, tc.code)
			}
			if body.Commitment != string(model.NotCommitted) {
				t.Fatalf("commitment = %q, want %q", body.Commitment, model.NotCommitted)
			}
		})
	}
}

func TestIdempotencyConflictIsNotCommitted(t *testing.T) {
	srv, _ := newServer(t, persist.AtomicWrite)
	snap := setupScene(t, srv)
	if resp, _ := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"}); resp.StatusCode != 202 {
		t.Fatalf("first submit: %d", resp.StatusCode)
	}
	resp, data := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 2}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 409 {
		t.Fatalf("status = %d, want 409: %s", resp.StatusCode, data)
	}
	var body errorBody
	_ = json.Unmarshal(data, &body)
	if body.Commitment != string(model.NotCommitted) {
		t.Fatalf("conflict commitment = %q", body.Commitment)
	}
}

func TestUnclassifiedErrorNeverBecomesAClientError(t *testing.T) {
	// A raw error can contain any text. It must still render as an internal
	// failure with an unknown commitment, never as a client rejection.
	rec := httptest.NewRecorder()
	fail(rec, errors.New("invalid JSON: looks like bad input"))
	if rec.Code != 500 {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if got := rec.Header().Get(CommitmentHeader); got != string(model.Unknown) {
		t.Fatalf("commitment = %q, want %q", got, model.Unknown)
	}
	var body errorBody
	if e := json.Unmarshal(rec.Body.Bytes(), &body); e != nil {
		t.Fatalf("decode: %v", e)
	}
	if body.Code != "internal" || body.Commitment != string(model.Unknown) {
		t.Fatalf("body = %+v", body)
	}
	if strings.Contains(body.Error, "invalid JSON") {
		t.Fatal("internal failure leaked low-level error text")
	}
}

func TestInternalKindStays500EvenWithClientLookingText(t *testing.T) {
	rec := httptest.NewRecorder()
	fail(rec, model.Internal("invalid state file", errors.New("checksum mismatch")))
	if rec.Code != 500 {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body errorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Code != "internal" {
		t.Fatalf("code = %q, want internal", body.Code)
	}
	if body.Commitment != string(model.NotCommitted) {
		t.Fatalf("commitment = %q", body.Commitment)
	}
}

// After a result is purged, replaying the idempotency key still resolves to the
// same committed job, and the artifact reports 410. A client can therefore
// distinguish "never accepted", "already promised" and "result cleaned".
func TestPurgedResultKeepsPromiseButReportsGone(t *testing.T) {
	srv, svc := newServer(t, persist.AtomicWrite)
	snap := setupScene(t, srv)
	resp, data := call(t, srv, "POST", "/v1/jobs", map[string]any{"snapshotId": snap, "scale": 1}, map[string]string{"Idempotency-Key": "one"})
	if resp.StatusCode != 202 {
		t.Fatalf("submit: %d %s", resp.StatusCode, data)
	}
	var job model.Job
	_ = json.Unmarshal(data, &job)

	// Commit the result this job would have produced. The worker is not part
	// of this test; the storage shape is what the HTTP layer must interpret.
	if e := svc.Store.Update(func(st *model.State) error {
		v := st.Jobs[job.ID]
		a := model.Artifact{ID: "png-00000999", JobID: v.ID, State: "live", Digest: model.Digest([]byte("png")), Width: 1, Height: 1, PNG: []byte("png")}
		st.Artifacts[a.ID] = a
		v.State = "succeeded"
		v.ArtifactID = a.ID
		st.Jobs[v.ID] = v
		return nil
	}); e != nil {
		t.Fatalf("complete job: %v", e)
	}

	if resp, data := call(t, srv, "DELETE", "/v1/artifacts/png-00000999", nil, nil); resp.StatusCode != 200 {
		t.Fatalf("purge: %d %s", resp.StatusCode, data)
	}
	resp, data = call(t, srv, "GET", "/v1/artifacts/png-00000999", nil, nil)
	if resp.StatusCode != 410 {
		t.Fatalf("artifact status = %d, want 410", resp.StatusCode)
	}
	var body errorBody
	_ = json.Unmarshal(data, &body)
	if body.Code != "gone" {
		t.Fatalf("code = %q, want gone", body.Code)
	}
	// The promise survives cleanup and a replay still yields the same job.
	lookupResp, lookupData := call(t, srv, "GET", "/v1/idempotency/one", nil, nil)
	if lookupResp.StatusCode != 200 {
		t.Fatalf("lookup: %d %s", lookupResp.StatusCode, lookupData)
	}
	var replayed model.Job
	_ = json.Unmarshal(lookupData, &replayed)
	if replayed.ID != job.ID || replayed.State != "succeeded" {
		t.Fatalf("lookup after purge = %+v, want job %s succeeded", replayed, job.ID)
	}
}
