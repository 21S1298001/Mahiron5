package api

import (
	"context"
	"net"
	"strings"

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
		user.Agent = userAgentWithAddress(request.RemoteAddr, request.UserAgent)
		user.URL = request.URL
	}
	return tuner.WithUser(ctx, user), id
}

func userAgentWithAddress(addr, agent string) string {
	return strings.Join(nonEmpty(remoteAddress(addr), agent), " ")
}

func remoteAddress(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}

func nonEmpty(values ...string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
