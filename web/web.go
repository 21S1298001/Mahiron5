package web

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/web/api"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

type WebConfig struct {
	Tuner *tuner.Tuner
}

func NewWeb(config WebConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	api, err := apigen.NewServer(api.NewHandler())
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))
	mux.HandleFunc("/", stream(config.Tuner))

	return mux, nil
}

func stream(t *tuner.Tuner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(200)

		t.StartStream(ctx, fmt.Sprintf("http-%s", r.RemoteAddr), w)

		slog.Info("stream ended", "remoteAddr", r.RemoteAddr)
	}
}
