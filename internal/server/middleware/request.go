package middleware

import (
	"context"
	"errors"
	"net/http"

	"github.com/21S1298001/mahiron/internal/contextvalue"
)

var requestInfoContextKey contextvalue.Key[*RequestInfo]

var ErrRequestInfoNotFound = errors.New("request info not found")

type RequestInfo struct {
	RemoteAddr string
	UserAgent  string
	URL        string
	Scheme     string
	Host       string
}

func RequestInfoMiddleware() *Middleware {
	return &Middleware{
		Name: "Request",
		Handler: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r.WithContext(contextvalue.With(r.Context(), requestInfoContextKey, &RequestInfo{
					RemoteAddr: r.RemoteAddr,
					UserAgent:  r.UserAgent(),
					URL:        r.URL.String(),
					Scheme:     requestScheme(r),
					Host:       requestHost(r),
				})))
			})
		},
	}
}

func requestScheme(r *http.Request) string {
	if scheme := r.Header.Get("X-Forwarded-Proto"); scheme != "" {
		return scheme
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func requestHost(r *http.Request) string {
	if host := r.Header.Get("X-Forwarded-Host"); host != "" {
		return host
	}
	return r.Host
}

func GetRequestInfo(ctx context.Context) (*RequestInfo, error) {
	if v, ok := contextvalue.Get(ctx, requestInfoContextKey); ok {
		return v, nil
	}
	return nil, ErrRequestInfoNotFound
}
