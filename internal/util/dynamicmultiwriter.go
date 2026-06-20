package util

import (
	"errors"
	"io"
	"os"
	"slices"
	"sync"
)

const dynamicMultiWriterBufferSize = 128

type DynamicMultiWriter struct {
	mutex       sync.RWMutex
	subscribers []*dynamicMultiWriterSubscriber
}

type dynamicMultiWriterSubscriber struct {
	writer io.Writer
	ch     chan []byte
	done   chan struct{}
	once   sync.Once
}

func NewDynamicMultiWriter(writers ...io.Writer) *DynamicMultiWriter {
	d := &DynamicMultiWriter{}
	for _, writer := range writers {
		d.Attach(writer)
	}
	return d
}

func IsExpectedStreamCloseError(err error) bool {
	return errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed)
}

func (d *DynamicMultiWriter) Attach(writer io.Writer) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	sub := &dynamicMultiWriterSubscriber{
		writer: writer,
		ch:     make(chan []byte, dynamicMultiWriterBufferSize),
		done:   make(chan struct{}),
	}
	d.subscribers = append(d.subscribers, sub)
	go sub.run(func() {
		d.detachSubscriber(sub, false)
	})
}

func (d *DynamicMultiWriter) Detach(writer io.Writer) {
	d.mutex.Lock()
	var sub *dynamicMultiWriterSubscriber
	for _, candidate := range d.subscribers {
		if candidate.writer == writer {
			sub = candidate
			break
		}
	}
	d.mutex.Unlock()

	if sub != nil {
		d.detachSubscriber(sub, true)
	}
}

func (d *DynamicMultiWriter) detachSubscriber(sub *dynamicMultiWriterSubscriber, wait bool) {
	d.mutex.Lock()
	for i, candidate := range d.subscribers {
		if candidate == sub {
			d.subscribers = slices.Delete(d.subscribers, i, i+1)
			sub.close()
			break
		}
	}
	d.mutex.Unlock()

	if wait {
		<-sub.done
	}
}

func (d *DynamicMultiWriter) Count() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.subscribers)
}

func (d *DynamicMultiWriter) Close() {
	d.mutex.Lock()
	subscribers := d.subscribers
	d.subscribers = nil
	d.mutex.Unlock()

	for _, sub := range subscribers {
		sub.close()
		if c, ok := sub.writer.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

func (d *DynamicMultiWriter) Write(p []byte) (n int, err error) {
	d.mutex.RLock()
	if len(d.subscribers) == 0 {
		d.mutex.RUnlock()
		return 0, io.ErrClosedPipe
	}

	chunk := append([]byte(nil), p...)
	for _, sub := range d.subscribers {
		sub.enqueue(chunk)
	}
	d.mutex.RUnlock()

	return len(p), nil
}

func (s *dynamicMultiWriterSubscriber) enqueue(chunk []byte) {
	select {
	case s.ch <- chunk:
		return
	default:
	}

	select {
	case <-s.ch:
	default:
	}

	select {
	case s.ch <- chunk:
	default:
	}
}

func (s *dynamicMultiWriterSubscriber) run(onError func()) {
	defer close(s.done)
	for chunk := range s.ch {
		written, err := s.writer.Write(chunk)
		if err != nil || written != len(chunk) {
			onError()
			return
		}
	}
}

func (s *dynamicMultiWriterSubscriber) close() {
	s.once.Do(func() {
		close(s.ch)
	})
}
