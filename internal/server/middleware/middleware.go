package middleware

import "net/http"

type MiddlewareFunc func(http.Handler) http.Handler

type Middleware struct {
	Name    string
	Handler MiddlewareFunc
}

func Synthesis(middlewares ...*Middleware) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		for i := range middlewares {
			h = middlewares[len(middlewares)-i-1].Handler(h)
		}
		return h
	}
}
