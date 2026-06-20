package api

import (
	"bytes"
	"context"
	"net/http"

	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func GetLog(_ context.Context, h *Handler) (apigen.GetLogRes, error) {
	if h.logStore == nil {
		return &apigen.GetLogDef{StatusCode: http.StatusServiceUnavailable}, nil
	}
	return &apigen.GetLogOK{Data: bytes.NewReader(h.logStore.Snapshot())}, nil
}

func GetLogStream(ctx context.Context, h *Handler) (apigen.GetLogStreamRes, error) {
	if h.logStore == nil {
		return &apigen.GetLogStreamDef{StatusCode: http.StatusServiceUnavailable}, nil
	}

	reader, unsubscribe := h.logStore.Subscribe()
	go func() {
		<-ctx.Done()
		unsubscribe()
		reader.Close()
	}()

	return &apigen.GetLogStreamOK{Data: reader}, nil
}
