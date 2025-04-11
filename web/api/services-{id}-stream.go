package api

import (
	"context"
	"io"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler) (apigen.GetServiceStreamRes, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		defer pr.Close()

		h.tuner.StartStream(ctx, "http-test", pw)
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.OptString{},
		Response: apigen.GetServiceStreamOK{
			Data: pr,
		},
	}, nil
}
