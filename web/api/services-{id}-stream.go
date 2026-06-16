package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"

	"github.com/21S1298001/Mahiron5/stream"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	service := h.serviceManager.GetServiceById(strconv.FormatInt(params.ID, 10))
	if service == nil {
		return &apigen.GetServiceStreamNotFound{}, nil
	}

	session, err := h.streamManager.GetOrCreate(ctx, service.ChannelType, service.ChannelId)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			return &apigen.GetServiceStreamNotFound{}, nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) {
			return &apigen.GetServiceStreamServiceUnavailable{}, nil
		}
		return nil, err
	}

	fo, fi := io.Pipe()
	go func() {
		defer fi.Close()
		if err := session.ServiceStream(ctx, service.ServiceId, fi); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("failed to stream service", "service", service.Id, "err", err)
		}
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.OptString{},
		Response: apigen.GetServiceStreamOK{
			Data: fo,
		},
	}, nil
}
