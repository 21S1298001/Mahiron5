package api

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"

	"github.com/21S1298001/Mahiron5/filter"
	"github.com/21S1298001/Mahiron5/server/middleware"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	requestInfo, err := middleware.GetRequestInfo(ctx)
	if err != nil {
		return nil, errors.New("request info not found")
	}

	service := h.serviceManager.GetServiceById(strconv.FormatInt(params.ID, 10))
	if service == nil {
		return &apigen.GetServiceStreamNotFound{}, nil
	}

	tuner := h.tunerManager.GetTunerByGroup(service.ChannelType)
	if tuner == nil {
		return nil, errors.New("tuner not found")
	}

	filter := filter.NewServiceFilter(ctx, service.ServiceId)
	fi, fo, err := filter.Pipe()
	if err != nil {
		return nil, err
	}

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			tuner.StartStream(ctx, requestInfo.RemoteAddr, fi)
		}()
		go func() {
			defer wg.Done()
			if err := filter.Filter(); err != nil {
				slog.Error("failed to apply filter", "err", err)
			}
		}()
		wg.Wait()
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.OptString{},
		Response: apigen.GetServiceStreamOK{
			Data: fo,
		},
	}, nil
}
