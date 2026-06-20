package program

import "context"

type ProgramManager struct {
	store ProgramStore
}

func NewProgramManager(store ProgramStore) *ProgramManager {
	return &ProgramManager{store: store}
}

func (m *ProgramManager) UpsertPrograms(ctx context.Context, programs []*Program) error {
	return m.store.UpsertAll(ctx, programs)
}

func (m *ProgramManager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *ProgramManager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *ProgramManager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	return m.store.DeleteEndedBefore(ctx, cutoff)
}

func (m *ProgramManager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	return m.store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs)
}

func (m *ProgramManager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }
