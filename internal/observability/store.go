package observability

import (
	"bytes"
	"io"
	"sync"
)

const defaultLogCapacity = 1000

type LogStore struct {
	mutex       sync.Mutex
	capacity    int
	records     [][]byte
	partial     []byte
	subscribers map[*subscriber]struct{}
}

type subscriber struct {
	ch      chan []byte
	closed  chan struct{}
	pending []byte
	once    sync.Once
}

type subscriptionReader struct {
	sub         *subscriber
	unsubscribe func()
}

func NewLogStore(capacity int) *LogStore {
	if capacity <= 0 {
		capacity = defaultLogCapacity
	}
	return &LogStore{
		capacity:    capacity,
		subscribers: map[*subscriber]struct{}{},
	}
}

func (s *LogStore) Write(p []byte) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	chunks := splitLogChunks(&s.partial, p)
	for _, chunk := range chunks {
		copied := append([]byte(nil), chunk...)
		s.records = append(s.records, copied)
		if overflow := len(s.records) - s.capacity; overflow > 0 {
			s.records = append([][]byte(nil), s.records[overflow:]...)
		}
		for sub := range s.subscribers {
			select {
			case sub.ch <- copied:
			default:
			}
		}
	}

	return len(p), nil
}

func (s *LogStore) Snapshot() []byte {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var buf bytes.Buffer
	for _, record := range s.records {
		buf.Write(record)
	}
	if len(s.partial) > 0 {
		buf.Write(s.partial)
	}
	return buf.Bytes()
}

func (s *LogStore) Subscribe() (io.ReadCloser, func()) {
	sub := &subscriber{
		ch:     make(chan []byte, 128),
		closed: make(chan struct{}),
	}

	s.mutex.Lock()
	s.subscribers[sub] = struct{}{}
	s.mutex.Unlock()

	unsubscribe := func() {
		sub.once.Do(func() {
			s.mutex.Lock()
			delete(s.subscribers, sub)
			s.mutex.Unlock()
			close(sub.closed)
		})
	}

	return &subscriptionReader{sub: sub, unsubscribe: unsubscribe}, unsubscribe
}

func (s *subscriber) Read(p []byte) (int, error) {
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		return n, nil
	}

	select {
	case data := <-s.ch:
		n := copy(p, data)
		if n < len(data) {
			s.pending = append(s.pending[:0], data[n:]...)
		}
		return n, nil
	case <-s.closed:
		return 0, io.EOF
	}
}

func (s *subscriber) Close() error {
	s.once.Do(func() {
		close(s.closed)
	})
	return nil
}

func (r *subscriptionReader) Read(p []byte) (int, error) {
	return r.sub.Read(p)
}

func (r *subscriptionReader) Close() error {
	r.unsubscribe()
	return r.sub.Close()
}

func splitLogChunks(partial *[]byte, p []byte) [][]byte {
	data := append(*partial, p...)
	*partial = nil
	if len(data) == 0 {
		return nil
	}

	var chunks [][]byte
	for len(data) > 0 {
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			*partial = append((*partial)[:0], data...)
			break
		}
		chunks = append(chunks, data[:idx+1])
		data = data[idx+1:]
	}
	return chunks
}
