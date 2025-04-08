package api

import (
	"context"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func CheckVersion(ctx context.Context, h *Handler) (apigen.CheckVersionRes, error) {
	return &apigen.Version{
		Current: apigen.NewOptString("5.0.0"),
		Latest:  apigen.NewOptString(""),
		Server:  apigen.NewOptString("mahiron"),
	}, nil
}
