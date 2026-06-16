package processor

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
)

func TestServiceScannerReportsMissingMirakcArib(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewServiceScanner().ScanServices(context.Background(), strings.NewReader(""), io.Discard)
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("ScanServices error = %v, want exec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "mirakc-arib is required for service scanning") {
		t.Fatalf("ScanServices error = %q, want scanning context", err.Error())
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
