package web

import (
	"net/http"
)

func NewWeb() http.Handler {
	return http.NewServeMux()
}
