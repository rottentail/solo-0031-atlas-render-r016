package bundle

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"encoding/json"
)

type Package struct {
	Format   string         `json:"format"`
	Version  int            `json:"version"`
	Snapshot model.Snapshot `json:"snapshot"`
	Digest   string         `json:"digest"`
}

func Export(s model.Snapshot) Package {
	p := Package{Format: "atlas-scene", Version: 1, Snapshot: s}
	p.Digest = model.CanonicalDigest(p.Snapshot)
	return p
}

// Validate checks an untrusted package before it is imported.
//
// persist.ValidateSnapshot reports integrity failures as plain errors because
// they are internal when they occur on this service's own state. Here the same
// failure is about the caller's package, so it is translated into a domain
// rejection at this boundary.
func Validate(p Package) error {
	if p.Format != "atlas-scene" || p.Version != 1 {
		return model.Invalid("unsupported scene package")
	}
	if model.CanonicalDigest(p.Snapshot) != p.Digest {
		return model.Invalid("package checksum mismatch")
	}
	if e := persist.ValidateSnapshot(p.Snapshot); e != nil {
		return model.Invalid("invalid package snapshot: %v", e)
	}
	return nil
}
func Decode(data []byte) (Package, error) {
	var p Package
	if len(data) > 12<<20 {
		return p, model.Invalid("package exceeds 12MiB")
	}
	if e := json.Unmarshal(data, &p); e != nil {
		return p, model.Invalid("invalid package JSON: %v", e)
	}
	return p, Validate(p)
}
