package middleware

import (
	"context"
	"errors"
	"net/http"
)

const requestInfoMiddlewareContextKey contextKey = "requestInfoMiddlewareContext"

var RequestInfoNotFoundError = errors.New("request info not found")

type RequestInfo struct {
	RemoteAddr string
}

func RequestInfoMiddleware() *Middleware {
	return &Middleware{
		Name: "Request",
		Handler: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestInfoMiddlewareContextKey, &RequestInfo{
					RemoteAddr: r.RemoteAddr,
				})))
			})
		},
	}
}

func GetRequestInfo(ctx context.Context) (*RequestInfo, error) {
	if v, ok := ctx.Value(requestInfoMiddlewareContextKey).(*RequestInfo); ok {
		return v, nil
	}
	return nil, RequestInfoNotFoundError
}
