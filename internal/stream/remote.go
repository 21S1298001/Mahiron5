package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
)

const remoteAvailabilityTimeout = 3 * time.Second

type RemoteClient struct {
	baseURL    string
	basicAuth  *config.BasicAuthConfig
	httpClient *http.Client
}

var newRemoteClient = NewRemoteClient

func NewRemoteClient(config config.RemoteConfig) *RemoteClient {
	return &RemoteClient{
		baseURL:    strings.TrimRight(config.URL, "/"),
		basicAuth:  config.BasicAuth,
		httpClient: http.DefaultClient,
	}
}

func (c *RemoteClient) CheckAvailable(ctx context.Context, channelType string) error {
	checkCtx, cancel := context.WithTimeout(ctx, remoteAvailabilityTimeout)
	defer cancel()

	req, err := c.newRequest(checkCtx, http.MethodGet, "tuners")
	if err != nil {
		slog.Warn("failed to build remote tuner status request", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Warn("failed to get remote tuner status", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("remote tuners status: %s", resp.Status)
		slog.Warn("remote tuner status returned non-success", "remote", c.baseURL, "type", channelType, "status", resp.Status)
		return err
	}

	var tuners []remoteTuner
	if err := json.NewDecoder(resp.Body).Decode(&tuners); err != nil {
		slog.Warn("failed to decode remote tuner status", "remote", c.baseURL, "type", channelType, "err", err)
		return err
	}
	for _, tuner := range tuners {
		if slices.Contains(tuner.Types, channelType) && tuner.IsAvailable && tuner.IsFree && !tuner.IsFault {
			return nil
		}
	}
	slog.Debug("remote tuner unavailable", "remote", c.baseURL, "type", channelType)
	return ErrTunerUnavailable
}

func (c *RemoteClient) ChannelStream(ctx context.Context, channelType, channel string, decode bool, dst io.Writer) error {
	return c.stream(ctx, decode, dst, "channels", channelType, channel, "stream")
}

func (c *RemoteClient) ServiceStream(ctx context.Context, channelType, channel string, serviceID uint16, decode bool, dst io.Writer) error {
	return c.stream(ctx, decode, dst, "channels", channelType, channel, "services", fmt.Sprint(serviceID), "stream")
}

func (c *RemoteClient) stream(ctx context.Context, decode bool, dst io.Writer, elems ...string) error {
	req, err := c.newRequest(ctx, http.MethodGet, elems...)
	if err != nil {
		return err
	}
	if decode {
		query := req.URL.Query()
		query.Set("decode", "1")
		req.URL.RawQuery = query.Encode()
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("remote stream status: %s", resp.Status)
	}
	_, err = io.Copy(dst, resp.Body)
	return err
}

func (c *RemoteClient) newRequest(ctx context.Context, method string, elems ...string) (*http.Request, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	parts := []string{strings.TrimRight(u.Path, "/")}
	for _, elem := range elems {
		parts = append(parts, url.PathEscape(elem))
	}
	u.Path = strings.Join(parts, "/")
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.basicAuth != nil {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}
	return req, nil
}

type remoteTuner struct {
	Types       []string `json:"types"`
	IsAvailable bool     `json:"isAvailable"`
	IsFree      bool     `json:"isFree"`
	IsFault     bool     `json:"isFault"`
}

type RemoteSessionConfig struct {
	Client       *RemoteClient
	Channel      *config.ChannelConfig
	RouteChannel *config.ChannelConfig
}

type RemoteSession struct {
	channel      *config.ChannelConfig
	client       *RemoteClient
	routeChannel *config.ChannelConfig
}

func NewRemoteSession(config RemoteSessionConfig) *RemoteSession {
	return &RemoteSession{
		channel:      config.Channel,
		client:       config.Client,
		routeChannel: config.RouteChannel,
	}
}

func (s *RemoteSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.client.ChannelStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
}

func (s *RemoteSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.client.ServiceStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, serviceID, decode, dst)
}

func (s *RemoteSession) ScanServices(context.Context, io.Writer) error {
	return ErrServiceScannerNotConfigured
}

func (s *RemoteSession) CollectEITS(context.Context, io.Writer) error {
	return ErrEITCollectorNotConfigured
}

func (s *RemoteSession) CollectEITPF(context.Context, io.Writer) error {
	return ErrEITCollectorNotConfigured
}

func (s *RemoteSession) Stop(context.Context) error {
	return nil
}
