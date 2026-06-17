package program

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
)

type ProgramManager struct {
	store ProgramStore
}

func NewProgramManager(store ProgramStore) *ProgramManager {
	if store == nil {
		store = NewMemoryProgramStore()
	}
	return &ProgramManager{store: store}
}

func (m *ProgramManager) Upsert(program *Program) error {
	return m.store.Upsert(program)
}

func (m *ProgramManager) UpsertEITSection(section *EITSection) error {
	for _, program := range section.Programs() {
		if err := m.store.Upsert(program); err != nil {
			return err
		}
		slog.Debug("upserted EIT program",
			"id", program.ID,
			"networkId", program.NetworkID,
			"serviceId", program.ServiceID,
			"eventId", program.EventID,
			"startAt", program.StartAt,
			"duration", program.Duration,
			"name", program.Name,
		)
	}
	return nil
}

func (m *ProgramManager) UpsertEITSectionJSON(data []byte) error {
	var section EITSection
	if err := json.Unmarshal(data, &section); err != nil {
		return err
	}
	return m.UpsertEITSection(&section)
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
		if err := m.UpsertEITSectionJSON(line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

func (m *ProgramManager) Get(id int64) (*Program, bool) {
	return m.store.Get(id)
}

func (m *ProgramManager) List(query Query) []*Program {
	return m.store.List(query)
}
