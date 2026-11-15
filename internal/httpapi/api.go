package httpapi

import (
	"atlas-render/internal/engine"
	"atlas-render/internal/model"
	"atlas-render/internal/service"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type API struct {
	Service *service.Service
	Worker  *engine.Worker
}

// CommitmentHeader is set on every error response and on replayed job
// creation. It is the transport form of model.Commitment and the only thing a
// client should use to decide whether a failure may be retried as new work.
const (
	CommitmentHeader = "X-Atlas-Commitment"
	ReplayHeader     = "Idempotent-Replay"
)

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorBody is the wire form of a classified failure. commitment is repeated
// in the body because a client behind a proxy may not see the header.
type errorBody struct {
	Code       string `json:"code"`
	Error      string `json:"error"`
	Commitment string `json:"commitment"`
}

// fail renders a classified error. Domain kinds select the status; internal
// failures are always 500 regardless of the text in the underlying error.
func fail(w http.ResponseWriter, e error) {
	commitment := model.CommitmentOf(e)
	status, code, message := 500, "internal", "operation could not be committed"
	if de, ok := model.ErrorOf(e); ok {
		code = de.Code()
		message = de.Error()
		switch de.Kind {
		case model.KindInvalid:
			status = 400
		case model.KindMissing:
			status = 404
		case model.KindConflict:
			status = 409
		case model.KindGone:
			status = 410
		case model.KindCapacity:
			status = 429
		case model.KindInternal:
			status = 500
			// Do not describe internal storage mechanics to clients; the
			// commitment is the actionable part.
			message = "operation could not be committed"
		}
	}
	w.Header().Set(CommitmentHeader, string(commitment))
	write(w, status, errorBody{Code: code, Error: message, Commitment: string(commitment)})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 12<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return model.Invalid("invalid JSON: %v", e)
	}
	var extra any
	if e := d.Decode(&extra); !errors.Is(e, io.EOF) {
		return model.Invalid("expected exactly one JSON value")
	}
	return nil
}
func (a API) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ready"}) })
	m.HandleFunc("PUT /v1/scenes/{id}", a.putScene)
	m.HandleFunc("GET /v1/scenes/{id}", a.getScene)
	m.HandleFunc("POST /v1/scenes/{id}/state", a.sceneState)
	m.HandleFunc("POST /v1/scenes/{id}/snapshots", a.freeze)
	m.HandleFunc("PUT /v1/assets/{id}", a.importAsset)
	m.HandleFunc("POST /v1/assets/{id}/retire", a.retireAsset)
	m.HandleFunc("GET /v1/snapshots/{id}", a.getSnapshot)
	m.HandleFunc("POST /v1/snapshots/{id}/revoke", a.revoke)
	m.HandleFunc("GET /v1/snapshots/{id}/package", a.exportPackage)
	m.HandleFunc("POST /v1/imports/{id}", a.importPackage)
	m.HandleFunc("POST /v1/jobs", a.submit)
	m.HandleFunc("GET /v1/jobs/{id}", a.getJob)
	m.HandleFunc("GET /v1/idempotency/{key}", a.lookupSubmission)
	m.HandleFunc("POST /v1/jobs/{id}/cancel", a.cancel)
	m.HandleFunc("POST /v1/jobs/{id}/retry", a.retry)
	m.HandleFunc("GET /v1/artifacts/{id}", a.download)
	m.HandleFunc("DELETE /v1/artifacts/{id}", a.purge)
	m.HandleFunc("POST /v1/compare", a.compare)
	m.HandleFunc("GET /v1/status", a.status)
	m.HandleFunc("GET /v1/events", a.events)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		m.ServeHTTP(w, r)
	})
}
