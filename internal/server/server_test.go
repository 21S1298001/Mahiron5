package server

import (
	"context"
	"net/http"
	"testing"
)

func TestNewHTTPServerUsesParentBaseContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := newHTTPServer(ctx, "127.0.0.1:0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	base := srv.BaseContext(nil)
	cancel()

	select {
	case <-base.Done():
	default:
		t.Fatal("server base context did not use parent context")
	}
}
