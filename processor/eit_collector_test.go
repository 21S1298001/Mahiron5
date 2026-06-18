package processor

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
)

func TestEITCollectorReportsMissingMirakcAribForEITS(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewEITCollector().CollectEITS(context.Background(), strings.NewReader(""), io.Discard)
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

func TestEITCollectorReportsMissingMirakcAribForEITPF(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewEITCollector().CollectEITPF(context.Background(), strings.NewReader(""), io.Discard)
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
