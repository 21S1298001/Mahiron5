package web

import (
	"net/http"

	"github.com/21S1298001/Mahiron5/job"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/web/api"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

type WebConfig struct {
	ServiceManager *service.ServiceManager
	ProgramManager *program.ProgramManager
	StreamManager  *stream.StreamManager
	TunerManager   *tuner.TunerManager
	JobManager     *job.JobManager
	EpgStaleAfter  int64
}

func NewWeb(config WebConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	api, err := apigen.NewServer(api.NewHandler(api.HandlerConfig{
		ServiceManager: config.ServiceManager,
		ProgramManager: config.ProgramManager,
		StreamManager:  config.StreamManager,
		TunerManager:   config.TunerManager,
		JobManager:     config.JobManager,
		EpgStaleAfter:  config.EpgStaleAfter,
	}))
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))

	return mux, nil
}
