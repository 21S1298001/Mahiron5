package util

import (
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type DynamicMultiWriter struct {
	mutex   *sync.RWMutex
	writers []io.Writer
}

func NewDynamicMultiWriter(writers ...io.Writer) *DynamicMultiWriter {
	return &DynamicMultiWriter{
		mutex:   &sync.RWMutex{},
		writers: writers,
	}
}

func (d *DynamicMultiWriter) Attach(writer io.Writer) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.writers = append(d.writers, writer)
}

func (d *DynamicMultiWriter) Detach(writer io.Writer) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for i, w := range d.writers {
		if w == writer {
			d.writers = slices.Delete(d.writers, i, i+1)
			break
		}
	}
}

func (d *DynamicMultiWriter) Count() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.writers)
}

func (d *DynamicMultiWriter) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, w := range d.writers {
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	d.writers = []io.Writer{}
}

func (d *DynamicMultiWriter) Write(p []byte) (n int, err error) {
	var meg multierror.Group
	for _, w := range d.writers {
		meg.Go(func() error {
			n, err = w.Write(p)
			if errors.Is(err, io.ErrClosedPipe) {
				d.Detach(w)
				return nil
			}
			if err != nil {
				return err
			}
			if n != len(p) {
				return io.ErrShortWrite
			}
			return nil
		})
	}
	if err := meg.Wait(); err != nil {
		return 0, err
	}
	if len(d.writers) == 0 {
		return 0, io.ErrClosedPipe
	}

	return len(p), nil
}
