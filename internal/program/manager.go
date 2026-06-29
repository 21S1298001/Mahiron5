package program

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
)

const programEventDelay = time.Second

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishProgramEvent(typ string, data map[string]any)
}

type ProgramManager struct {
	store      ProgramStore
	events     eventPublisher
	eventMu    sync.Mutex
	eventTimer *time.Timer
	eventQueue []programEvent
}

type programEvent struct {
	typ      string
	program  *Program
	removeID int64
}

func NewProgramManager(store ProgramStore, events ...eventPublisher) *ProgramManager {
	m := &ProgramManager{store: store}
	if len(events) > 0 {
		m.events = events[0]
	}
	return m
}

func (m *ProgramManager) UpsertPrograms(ctx context.Context, programs []*Program) error {
	source := observability.EPGMetricSource(ctx)
	attempted := nonNilProgramCount(programs)
	ids := make([]int64, 0, len(programs))
	for _, p := range programs {
		if p == nil {
			continue
		}
		ids = append(ids, p.ID)
	}
	existingPrograms, err := m.store.ListByIDs(ctx, ids)
	if err != nil {
		return err
	}
	before := make(map[int64]*Program, len(existingPrograms))
	for _, p := range existingPrograms {
		before[p.ID] = p
	}
	if err := m.store.UpsertAll(ctx, programs); err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		return err
	}
	afterPrograms, err := m.store.ListByIDs(ctx, ids)
	if err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		return err
	}
	changed := 0
	for _, p := range afterPrograms {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		switch {
		case !ok:
			changed++
			m.enqueueProgramEvent(eventTypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			changed++
			m.enqueueProgramEvent(eventTypeUpdate, p)
		}
	}
	observability.RecordEPGProgramsUpserted(ctx, source, "success", int64(changed))
	return nil
}

func (m *ProgramManager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *ProgramManager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *ProgramManager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	source := observability.EPGMetricSource(ctx)
	removed, err := m.store.ListEndedIDsBefore(ctx, cutoff)
	if err != nil {
		return err
	}
	if err := m.store.DeleteEndedBefore(ctx, cutoff); err != nil {
		observability.RecordEPGProgramsDeleted(ctx, source, "error", int64(len(removed)))
		return err
	}
	for _, id := range removed {
		m.enqueueProgramRemoveEvent(id)
	}
	observability.RecordEPGProgramsDeleted(ctx, source, "success", int64(len(removed)))
	return nil
}

func (m *ProgramManager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	source := observability.EPGMetricSource(ctx)
	attempted := nonNilProgramCount(programs)
	beforeList, err := m.store.ListByServiceFrom(ctx, networkID, serviceID, from)
	if err != nil {
		return err
	}
	before := map[int64]*Program{}
	for _, p := range beforeList {
		before[p.ID] = p
	}
	if err := m.store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs); err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		observability.RecordEPGProgramsDeleted(ctx, source, "error", int64(len(beforeList)))
		return err
	}
	changed := 0
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		delete(before, p.ID)
		switch {
		case !ok:
			changed++
			m.enqueueProgramEvent(eventTypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			changed++
			m.enqueueProgramEvent(eventTypeUpdate, p)
		}
	}
	for id := range before {
		m.enqueueProgramRemoveEvent(id)
	}
	observability.RecordEPGProgramsUpserted(ctx, source, "success", int64(changed))
	observability.RecordEPGProgramsDeleted(ctx, source, "success", int64(len(before)))
	return nil
}

func (m *ProgramManager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }

func nonNilProgramCount(programs []*Program) int {
	count := 0
	for _, p := range programs {
		if p != nil {
			count++
		}
	}
	return count
}

func (m *ProgramManager) enqueueProgramEvent(typ string, p *Program) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: typ, program: p})
}

func (m *ProgramManager) enqueueProgramRemoveEvent(id int64) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: eventTypeRemove, removeID: id})
}

func (m *ProgramManager) enqueueEvent(event programEvent) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.eventQueue = append(m.eventQueue, event)
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
		if event.typ == eventTypeRemove {
			m.events.PublishProgramEvent(event.typ, map[string]any{"id": event.removeID})
		} else {
			m.events.PublishProgramEvent(event.typ, event.program.EventData())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
