package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/server"
	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
	"github.com/asticode/go-astits"
)

func main() {
	serverConfig, err := config.LoadAndParseSystemConfig("server.yml")
	if err != nil {
		slog.Error("failed to load config", "err", err)
	}

	level := slog.LevelInfo
	switch serverConfig.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	addresses := make([]server.ListenAddress, len(serverConfig.Addresses))
	for i, addr := range serverConfig.Addresses {
		addresses[i] = server.ListenAddress{
			Http: addr.Http,
			Unix: addr.Unix,
		}
	}

	handlers := map[string]http.HandlerFunc{
		"/": stream,
	}

	slog.Info("starting servers")
	s := server.NewServer(addresses, handlers)
	s.ListenAndServe()

	<-signalCtx.Done()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("shutting down servers")
	s.Shutdown(timeoutCtx)

	slog.Info("all servers shut down")
	slog.Info("exiting")
	os.Exit(0)
}

func stream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	slog.Info("start streaming")
	resp, err := http.Get("http://v6.haruka.dns.ggrel.net:40772/api/services/3273601024/stream")
	if err != nil {
		slog.Error("failed to get stream", "err", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("failed to get stream", "status", resp.StatusCode)
		return
	}
	defer resp.Body.Close()

	slog.Info("successfully got stream", "status", resp.StatusCode)

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Content-Length", resp.Header.Get("Content-Length"))
	w.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		slog.Info("HEAD request, returning")
		return
	}

	slog.Info("streaming data")

	pr, pw := io.Pipe()
	defer pr.Close()

	dmwr := dynamicmultiwriter.New(
		map[string]io.Writer{
			"http":   w,
			"parser": pw,
		},
	)

	defer dmwr.CloseAll()

	copyCh := make(chan error)
	go func() {
		_, err := io.Copy(dmwr, resp.Body)
		if err != nil {
			copyCh <- err
			return
		}
		copyCh <- nil
	}()

	go func() {
		dmx := astits.NewDemuxer(ctx, bufio.NewReader(pr))
		for {
			_, err := dmx.NextData()
			if err != nil {
				continue
			}

			// if d.EIT != nil {
			// 	slog.Info("EIT detected")
			// 	for _, e := range d.EIT.Events {
			// 		slog.Info("Event detected", "event_id", e.EventID)
			// 		for _, d := range e.Descriptors {
			// 			if d.Content != nil {
			// 				for _, i := range d.Content.Items {
			// 					slog.Info("Content items", "item", i)
			// 				}
			// 			}
			// 		}
			// 	}
			// }
		}
	}()

	slog.Info("waiting for copy to finish")
	select {
	case <-ctx.Done():
		dmwr.CloseAll()
		slog.Info("connection closed by client")
		return

	case err := <-copyCh:
		if err == nil {
			slog.Info("copy finished")
			return
		}

		if opErr, ok := err.(*net.OpError); ok {
			if sysErr, ok := opErr.Err.(*os.SyscallError); ok && sysErr.Err == syscall.EPIPE {
				slog.Info("connection closed by remote")
				return
			}
		}
		slog.Error("copy error", "err", err)
		return
	}
}
