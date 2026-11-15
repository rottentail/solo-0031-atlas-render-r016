package bundle

import (
	"atlas-render/internal/model"
	"testing"
)

func sample() Package {
	doc := model.Document{
		Width: 4, Height: 4, Background: "#000000",
		Layers: []model.Layer{{
			ID: "plate", Kind: "rectangle", Width: 2, Height: 2, Opacity: 1,
			Transform: model.Transform{ScaleX: 1, ScaleY: 1},
			Fill:      model.Paint{Kind: "solid", Color: "#FFFFFF"},
		}},
	}
	snap := model.Snapshot{ID: "snap-1", SceneID: "scene-1", Revision: 1, State: "active", Document: doc, Assets: map[string]model.Asset{}}
	snap.Digest = model.SnapshotDigest(snap)
	return Package{Format: "atlas-scene", Version: 1, Snapshot: snap, Digest: model.CanonicalDigest(snap)}
}

// A corrupt package is a client mistake and must stay a NotCommitted domain
// rejection, even though the shared integrity validator reports plain errors.
func TestCorruptPackageIsInvalidNotInternal(t *testing.T) {
	p := sample()
	p.Snapshot.Document.Width = 99 // breaks the snapshot digest
	e := Validate(p)
	if e == nil {
		t.Fatal("expected rejection")
	}
	kind, ok := model.KindOf(e)
	if !ok || kind != model.KindInvalid {
		t.Fatalf("kind = %v (%v), want invalid", kind, e)
	}
	if got := model.CommitmentOf(e); got != model.NotCommitted {
		t.Fatalf("commitment = %q, want %q", got, model.NotCommitted)
	}
}

func TestValidPackagePasses(t *testing.T) {
	if e := Validate(sample()); e != nil {
		t.Fatalf("valid package rejected: %v", e)
	}
}
