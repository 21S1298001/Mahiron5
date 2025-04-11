package web

import (
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
	api, err := apigen.NewServer(api.NewHandler(api.HandlerConfig{
		Tuner: config.Tuner,
	}))
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))

	return mux, nil
}
