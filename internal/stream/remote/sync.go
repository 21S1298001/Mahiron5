package remote

import (
	"context"
	"log/slog"
	"time"
)

const (
	programEventSyncInitialBackoff = time.Second
	programEventSyncMaxBackoff     = time.Minute
)

// RunProgramEventSync streams program events from the remote and feeds them
// into the updater, reconnecting with exponential backoff until the context
// is canceled.
func RunProgramEventSync(ctx context.Context, name string, client *Client, updater ProgramUpdater) {
	backoff := programEventSyncInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		slog.Debug("starting remote program event sync", "remote", name)
		err := client.StreamProgramEvents(ctx, updater)
		if err := ctx.Err(); err != nil {
			return
		}
		slog.Warn("remote program event sync stopped", "remote", name, "err", err, "retryIn", backoff)
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, programEventSyncMaxBackoff)
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
