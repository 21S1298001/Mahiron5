package api

import (
	"context"

	"github.com/21S1298001/mahiron/internal/version"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

const currentVersion = version.Current

func CheckVersion(ctx context.Context, h *Handler) (apigen.CheckVersionRes, error) {
	return &apigen.Version{
		Current: apigen.NewOptString(currentVersion),
		Latest:  apigen.NewOptString(""),
		Server:  apigen.NewOptString(version.Server),
	}, nil
}
