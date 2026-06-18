package program

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
)

type ProgramManager struct {
	store ProgramStore
}

func NewProgramManager(store ProgramStore) *ProgramManager {
	return &ProgramManager{store: store}
}

func (m *ProgramManager) UpsertEITSection(ctx context.Context, section *EITSection) error {
	return m.store.UpsertAll(ctx, section.Programs())
}

func (m *ProgramManager) UpsertEITSectionJSON(ctx context.Context, data []byte) error {
	var section EITSection
	if err := json.Unmarshal(data, &section); err != nil {
		return err
	}
	return m.UpsertEITSection(ctx, &section)
}

func (m *ProgramManager) ReadEITJSONL(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := m.UpsertEITSectionJSON(ctx, line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return ctx.Err()
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
