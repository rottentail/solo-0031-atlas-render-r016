package service

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/scene"
)

func (s *Service) ImportAsset(id string, data []byte) (model.Asset, error) {
	var out model.Asset
	if !scene.ValidID(id) {
		return out, model.Invalid("invalid asset id")
	}
	img, e := codec.Decode(data)
	if e != nil {
		return out, e
	}
	candidate := model.Asset{ID: id, Digest: model.Digest(data), State: "available", Width: img.Bounds().Dx(), Height: img.Bounds().Dy(), PNG: data}
	e = s.Store.Update(func(st *model.State) error {
		if old, ok := st.Assets[id]; ok {
			if old.Digest == candidate.Digest && old.State == "available" {
				out = old
				return nil
			}
			return model.Conflict("asset id already exists")
		}
		st.Assets[id] = candidate
		st.Emit("asset.imported", id)
		out = candidate
		return nil
	})
	if e != nil {
		return model.Asset{}, e
	}
	return out, nil
}
func (s *Service) RetireAsset(id string) error {
	return s.Store.Update(func(st *model.State) error {
		a, ok := st.Assets[id]
		if !ok {
			return model.Missing("asset not found")
		}
		if a.State == "retired" {
			return nil
		}
		a.State = "retired"
		st.Assets[id] = a
		st.Emit("asset.retired", id)
		return nil
	})
}
