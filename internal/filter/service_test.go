package filter

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/processor"
)

func TestServiceFilterReportsMissingMirakcArib(t *testing.T) {
	replaceVar(t, &lookPath, func(file string) (string, error) {
		return "", exec.ErrNotFound
	})

	err := NewServiceFilter().FilterService(context.Background(), 1024, strings.NewReader(""), io.Discard)
	if !errors.Is(err, processor.ErrMirakcAribRequired) {
		t.Fatalf("FilterService error = %v, want ErrMirakcAribRequired", err)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("FilterService error = %v, want exec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "service filtering") {
		t.Fatalf("FilterService error = %q, want filtering context", err.Error())
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
