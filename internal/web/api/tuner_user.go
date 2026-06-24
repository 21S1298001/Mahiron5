package api

import (
	"context"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/server/middleware"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/google/uuid"
)

func tunerUserContext(ctx context.Context, priority apigen.OptInt, decode bool, channel *config.ChannelConfig, networkID, serviceID *uint16) (context.Context, string) {
	id := uuid.NewString()
	value := 0
	if configured, ok := priority.Get(); ok {
		value = configured
	}
	user := tuner.User{
		ID: id, Priority: value, DisableDecoder: !decode,
		StreamSetting: tuner.StreamSetting{Channel: channel, NetworkID: networkID, ServiceID: serviceID},
	}
	if request, err := middleware.GetRequestInfo(ctx); err == nil {
		user.Agent = request.UserAgent
		user.URL = request.URL
	}
	return tuner.WithUser(ctx, user), id
}
