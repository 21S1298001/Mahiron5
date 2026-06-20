package program

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/eventhub"
)

const programEventDelay = time.Second

type ProgramManager struct {
	store      ProgramStore
	events     eventhub.Publisher
	eventMu    sync.Mutex
	eventTimer *time.Timer
	eventQueue []programEvent
}

type programEvent struct {
	typ  string
	data any
}

func NewProgramManager(store ProgramStore, events ...eventhub.Publisher) *ProgramManager {
	m := &ProgramManager{store: store}
	if len(events) > 0 {
		m.events = events[0]
	}
	return m
}

func (m *ProgramManager) UpsertPrograms(ctx context.Context, programs []*Program) error {
	before := make(map[int64]*Program, len(programs))
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok, err := m.store.Get(ctx, p.ID)
		if err != nil {
			return err
		}
		if ok {
			before[p.ID] = existing
		}
	}
	if err := m.store.UpsertAll(ctx, programs); err != nil {
		return err
	}
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		switch {
		case !ok:
			m.enqueueEvent(eventhub.TypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			m.enqueueEvent(eventhub.TypeUpdate, p)
		}
	}
	return nil
}

func (m *ProgramManager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *ProgramManager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *ProgramManager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	programs, err := m.store.List(ctx, Query{})
	if err != nil {
		return err
	}
	removed := make([]int64, 0)
	for _, p := range programs {
		if p.StartAt+int64(p.Duration) < cutoff {
			removed = append(removed, p.ID)
		}
	}
	if err := m.store.DeleteEndedBefore(ctx, cutoff); err != nil {
		return err
	}
	for _, id := range removed {
		m.enqueueEvent(eventhub.TypeRemove, map[string]int64{"id": id})
	}
	return nil
}

func (m *ProgramManager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	beforeList, err := m.store.List(ctx, Query{NetworkID: &networkID, ServiceID: &serviceID})
	if err != nil {
		return err
	}
	before := map[int64]*Program{}
	for _, p := range beforeList {
		if p.StartAt >= from {
			before[p.ID] = p
		}
	}
	if err := m.store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs); err != nil {
		return err
	}
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		delete(before, p.ID)
		switch {
		case !ok:
			m.enqueueEvent(eventhub.TypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			m.enqueueEvent(eventhub.TypeUpdate, p)
		}
	}
	for id := range before {
		m.enqueueEvent(eventhub.TypeRemove, map[string]int64{"id": id})
	}
	return nil
}

func (m *ProgramManager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }

func (m *ProgramManager) enqueueEvent(typ string, data any) {
	if m.events == nil {
		return
	}
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.eventQueue = append(m.eventQueue, programEvent{typ: typ, data: data})
	if m.eventTimer != nil {
		m.eventTimer.Reset(programEventDelay)
		return
	}
	m.eventTimer = time.AfterFunc(programEventDelay, m.flushEvents)
}

func (m *ProgramManager) flushEvents() {
	m.eventMu.Lock()
	queue := append([]programEvent(nil), m.eventQueue...)
	m.eventQueue = nil
	m.eventTimer = nil
	m.eventMu.Unlock()

	for _, event := range queue {
		m.events.PublishEvent(eventhub.ResourceProgram, event.typ, event.data)
		time.Sleep(10 * time.Millisecond)
	}
}
