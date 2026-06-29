package apigen

import (
	"io"
	"net/http"
)

type flushWriter struct {
	writer  io.Writer
	flusher http.Flusher
}

func streamFlushWriter(w http.ResponseWriter) io.Writer {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return w
	}
	return flushWriter{writer: w, flusher: flusher}
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		w.flusher.Flush()
	}
	return n, err
}
