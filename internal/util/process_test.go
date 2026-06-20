package util

import (
	"context"
	"testing"
	"time"
)

func TestProcessStopReturnsBeforeContextDeadline(t *testing.T) {
	process := NewProcess("sleep 10")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := process.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop took %s, want it to return before the context deadline", elapsed)
	}
}

func TestProcessWaitAfterStopIsSafe(t *testing.T) {
	process := NewProcess("sleep 10")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := process.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
}
