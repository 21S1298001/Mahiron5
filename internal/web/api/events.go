package api

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/21S1298001/Mahiron5/internal/program"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func GetEvents(ctx context.Context, h *Handler) (apigen.GetEventsRes, error) {
	events, err := currentEvents(ctx, h)
	if err != nil {
		return nil, err
	}
	res := apigen.GetEventsOKApplicationJSON(events)
	return &res, nil
}

func GetEventsStream(ctx context.Context, h *Handler, params apigen.GetEventsStreamParams) (apigen.GetEventsStreamRes, error) {
	return &apigen.GetEventsStreamOK{
		Data: newEventsStreamReader(ctx, h, params),
	}, nil
}

func currentEvents(ctx context.Context, h *Handler) ([]apigen.Event, error) {
	now := apigen.UnixtimeMS(time.Now().UnixMilli())
	events := make([]apigen.Event, 0)

	if h.programManager != nil {
		programs, err := h.programManager.List(ctx, program.Query{})
		if err != nil {
			return nil, err
		}
		for _, p := range programs {
			event, err := apiEvent(apigen.EventResourceProgram, apiProgram(p), now)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}

	if h.serviceManager != nil {
		services, err := h.serviceManager.GetServices(ctx)
		if err != nil {
			return nil, err
		}
		for _, service := range services {
			event, err := apiEvent(apigen.EventResourceService, apiService(h, service, true), now)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}

	if h.tunerManager != nil {
		for _, status := range h.tunerManager.Statuses() {
			event, err := apiEvent(apigen.EventResourceTuner, apiTuner(status), now)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}

	return events, nil
}

func apiEvent(resource apigen.EventResource, payload any, now apigen.UnixtimeMS) (apigen.Event, error) {
	data, err := apiEventData(payload)
	if err != nil {
		return apigen.Event{}, err
	}
	return apigen.Event{
		Resource: resource,
		Type:     apigen.EventTypeUpdate,
		Data:     data,
		Time:     now,
	}, nil
}

func apiEventData(payload any) (apigen.EventData, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var data apigen.EventData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func matchesEventStreamParams(event apigen.Event, params apigen.GetEventsStreamParams) bool {
	if resource, ok := params.Resource.Get(); ok && string(event.Resource) != string(resource) {
		return false
	}
	if typ, ok := params.Type.Get(); ok && string(event.Type) != string(typ) {
		return false
	}
	return true
}

func newEventsStreamReader(ctx context.Context, h *Handler, params apigen.GetEventsStreamParams) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		if err := writeEventsOpenJSONArraySnapshot(ctx, writer, h, params); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		<-ctx.Done()
		_ = writer.Close()
	}()
	return reader
}

func writeEventsOpenJSONArraySnapshot(ctx context.Context, w io.Writer, h *Handler, params apigen.GetEventsStreamParams) error {
	if _, err := io.WriteString(w, "[\n"); err != nil {
		return err
	}
	events, err := currentEvents(ctx, h)
	if err != nil {
		return err
	}
	for _, event := range events {
		if !matchesEventStreamParams(event, params) {
			continue
		}
		if err := writeOpenJSONArrayEvent(w, event); err != nil {
			return err
		}
	}
	return nil
}

func writeOpenJSONArrayEvent(w io.Writer, event apigen.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n', ',', '\n'))
	return err
}
