package util

import (
	"errors"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
)

const dynamicMultiWriterBufferSize = 128

type DynamicMultiWriter struct {
	mutex       sync.RWMutex
	pool        sync.Pool
	subscribers []*dynamicMultiWriterSubscriber
}

type dynamicMultiWriterSubscriber struct {
	writer io.Writer
	ch     chan *dynamicMultiWriterChunk
	done   chan struct{}
	once   sync.Once
}

type dynamicMultiWriterChunk struct {
	refs int32
	data []byte
	pool *sync.Pool
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
		ch:     make(chan *dynamicMultiWriterChunk, dynamicMultiWriterBufferSize),
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

	chunk := d.newChunk(p, len(d.subscribers))
	for _, sub := range d.subscribers {
		sub.enqueue(chunk)
	}
	d.mutex.RUnlock()

	return len(p), nil
}

func (d *DynamicMultiWriter) newChunk(p []byte, refs int) *dynamicMultiWriterChunk {
	chunk, _ := d.pool.Get().(*dynamicMultiWriterChunk)
	if chunk == nil {
		chunk = &dynamicMultiWriterChunk{pool: &d.pool}
	}
	if cap(chunk.data) < len(p) {
		chunk.data = make([]byte, len(p))
	} else {
		chunk.data = chunk.data[:len(p)]
	}
	copy(chunk.data, p)
	atomic.StoreInt32(&chunk.refs, int32(refs))
	return chunk
}

func (c *dynamicMultiWriterChunk) release() {
	if atomic.AddInt32(&c.refs, -1) != 0 {
		return
	}
	pool := c.pool
	c.data = c.data[:0]
	pool.Put(c)
}

func (s *dynamicMultiWriterSubscriber) enqueue(chunk *dynamicMultiWriterChunk) {
	select {
	case s.ch <- chunk:
		return
	default:
	}

	select {
	case dropped := <-s.ch:
		dropped.release()
	default:
	}

	select {
	case s.ch <- chunk:
	default:
		chunk.release()
	}
}

func (s *dynamicMultiWriterSubscriber) run(onError func()) {
	defer close(s.done)
	defer s.drain()
	for chunk := range s.ch {
		want := len(chunk.data)
		written, err := s.writer.Write(chunk.data)
		chunk.release()
		if err != nil || written != want {
			onError()
			return
		}
	}
}

func (s *dynamicMultiWriterSubscriber) drain() {
	for {
		select {
		case chunk, ok := <-s.ch:
			if !ok {
				return
			}
			chunk.release()
		default:
			return
		}
	}
}

func (s *dynamicMultiWriterSubscriber) close() {
	s.once.Do(func() {
		close(s.ch)
	})
}
