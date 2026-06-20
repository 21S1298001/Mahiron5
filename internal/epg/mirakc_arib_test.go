package epg

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
)

func TestMirakcAribCollectorCommands(t *testing.T) {
	if eitsCollectorCommand != "mirakc-arib collect-eits" {
		t.Fatalf("EITS command = %q", eitsCollectorCommand)
	}
	if eitpfCollectorCommand != "mirakc-arib collect-eitpf --streaming" {
		t.Fatalf("EITPF command = %q", eitpfCollectorCommand)
	}
}

func TestMirakcAribCollectorReportsMissingMirakcAribForEITS(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewMirakcAribCollector().CollectEITS(context.Background(), strings.NewReader(""), io.Discard)
	if !errors.Is(err, ErrMirakcAribRequired) {
		t.Fatalf("CollectEITS error = %v, want ErrMirakcAribRequired", err)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("CollectEITS error = %v, want exec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "EITS collection") {
		t.Fatalf("CollectEITS error = %q, want EITS context", err.Error())
	}
}

func TestMirakcAribCollectorReportsMissingMirakcAribForEITPF(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewMirakcAribCollector().CollectEITPF(context.Background(), strings.NewReader(""), io.Discard)
	if !errors.Is(err, ErrMirakcAribRequired) {
		t.Fatalf("CollectEITPF error = %v, want ErrMirakcAribRequired", err)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("CollectEITPF error = %v, want exec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "EITPF collection") {
		t.Fatalf("CollectEITPF error = %q, want EITPF context", err.Error())
	}
}

func replaceVar[T any](t *testing.T, target *T, value T) {
	t.Helper()
	orig := *target
	*target = value
	t.Cleanup(func() {
		*target = orig
	})
}
