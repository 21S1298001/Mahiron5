package program

import "sync"

type ProgramStore interface {
	Upsert(*Program) error
	Get(int64) (*Program, bool)
	List(Query) []*Program
}

type MemoryProgramStore struct {
	mu       sync.RWMutex
	programs map[int64]*Program
}

func NewMemoryProgramStore() *MemoryProgramStore {
	return &MemoryProgramStore{
		programs: map[int64]*Program{},
	}
}

func (s *MemoryProgramStore) Upsert(program *Program) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.programs[program.ID] = cloneProgram(program)
	return nil
}

func (s *MemoryProgramStore) Get(id int64) (*Program, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	program, ok := s.programs[id]
	return cloneProgram(program), ok
}

func (s *MemoryProgramStore) List(query Query) []*Program {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Program, 0, len(s.programs))
	for _, program := range s.programs {
		if query.ID != nil && program.ID != *query.ID {
			continue
		}
		if query.NetworkID != nil && program.NetworkID != *query.NetworkID {
			continue
		}
		if query.ServiceID != nil && program.ServiceID != *query.ServiceID {
			continue
		}
		if query.EventID != nil && program.EventID != *query.EventID {
			continue
		}
		result = append(result, cloneProgram(program))
	}
	Sort(result)
	return result
}
