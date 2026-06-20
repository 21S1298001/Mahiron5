package stream

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
)

func TestRemoteClientCheckAvailableAndBasicAuth(t *testing.T) {
	var auth string
	var hasDeadline bool
	client := NewRemoteClient(config.RemoteConfig{
		URL:       "http://remote.local/api",
		BasicAuth: &config.BasicAuthConfig{Username: "user", Password: "pass"},
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		_, hasDeadline = r.Context().Deadline()
		if r.URL.Path != "/api/tuners" {
			t.Fatalf("path = %s, want /api/tuners", r.URL.Path)
		}
		return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":true,"isFault":false}]`), nil
	})}
	if err := client.CheckAvailable(context.Background(), "GR"); err != nil {
		t.Fatal(err)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	if auth != wantAuth {
		t.Fatalf("Authorization = %q, want %q", auth, wantAuth)
	}
	if !hasDeadline {
		t.Fatal("remote availability request context has no deadline")
	}
}

func TestRemoteClientNoAuthAndUnavailable(t *testing.T) {
	var auth string
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		auth = r.Header.Get("Authorization")
		return stringResponse(http.StatusOK, `[{"types":["GR"],"isAvailable":true,"isFree":false,"isFault":false}]`), nil
	})}
	if err := client.CheckAvailable(context.Background(), "GR"); err != ErrTunerUnavailable {
		t.Fatalf("CheckAvailable error = %v, want ErrTunerUnavailable", err)
	}
	if auth != "" {
		t.Fatalf("Authorization = %q, want empty", auth)
	}
}

func TestRemoteSessionStreamsChannelAndService(t *testing.T) {
	paths := []string{}
	queries := []string{}
	client := NewRemoteClient(config.RemoteConfig{URL: "http://remote.local/api"})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		queries = append(queries, r.URL.RawQuery)
		switch r.URL.Path {
		case "/api/channels/GR/27/stream":
			return stringResponse(http.StatusOK, "channel-ts"), nil
		case "/api/channels/GR/27/services/1024/stream":
			return stringResponse(http.StatusOK, "service-ts"), nil
		default:
			return stringResponse(http.StatusNotFound, ""), nil
		}
	})}

	session := NewRemoteSession(RemoteSessionConfig{
		Client:       client,
		RouteChannel: &config.ChannelConfig{Type: "GR", Channel: "27"},
	})

	var channelOut bytes.Buffer
	if err := session.ChannelStream(context.Background(), false, &channelOut); err != nil {
		t.Fatal(err)
	}
	var serviceOut bytes.Buffer
	if err := session.ServiceStream(context.Background(), 1024, true, &serviceOut); err != nil {
		t.Fatal(err)
	}
	if channelOut.String() != "channel-ts" || serviceOut.String() != "service-ts" {
		t.Fatalf("streams = %q/%q", channelOut.String(), serviceOut.String())
	}
	if len(paths) != 2 || paths[0] != "/api/channels/GR/27/stream" || paths[1] != "/api/channels/GR/27/services/1024/stream" {
		t.Fatalf("paths = %#v", paths)
	}
	if len(queries) != 2 || queries[0] != "" || queries[1] != "decode=1" {
		t.Fatalf("queries = %#v", queries)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func stringResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}
