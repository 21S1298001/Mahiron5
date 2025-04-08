package web

import "github.com/21S1298001/Mahiron5/server"

func NewWeb() server.Handlers {
	return server.Handlers{
		"/": nil,
	}
}
