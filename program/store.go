package program

import "context"

type ProgramStore interface {
	UpsertAll(ctx context.Context, programs []*Program) error
	Get(ctx context.Context, id int64) (*Program, bool, error)
	List(ctx context.Context, query Query) ([]*Program, error)
	DeleteEndedBefore(ctx context.Context, cutoff int64) error
	ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error
	Count(ctx context.Context) (int, error)
}
