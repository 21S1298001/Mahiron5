package program

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/21S1298001/Mahiron5/db/gen"
)

type sqliteStore struct {
	db *sql.DB
	q  *gen.Queries
}

func NewSQLiteStore(db *sql.DB) ProgramStore {
	return &sqliteStore{
		db: db,
		q:  gen.New(db),
	}
}

func (s *sqliteStore) UpsertAll(ctx context.Context, programs []*Program) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO programs (id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmt.Close()

	for _, p := range programs {
		var name, desc *string
		if p.Name != "" {
			name = &p.Name
		}
		if p.Description != "" {
			desc = &p.Description
		}

		genresJSON, err := json.Marshal(p.Genres)
		if err != nil {
			return fmt.Errorf("marshal program %d genres: %w", p.ID, err)
		}
		videoJSON, err := json.Marshal(p.Video)
		if err != nil {
			return fmt.Errorf("marshal program %d video: %w", p.ID, err)
		}
		audiosJSON, err := json.Marshal(p.Audios)
		if err != nil {
			return fmt.Errorf("marshal program %d audios: %w", p.ID, err)
		}

		var genresStr, videoStr, audiosStr *string
		if len(genresJSON) > 0 && string(genresJSON) != "null" {
			v := string(genresJSON)
			genresStr = &v
		}
		if len(videoJSON) > 0 && string(videoJSON) != "null" {
			v := string(videoJSON)
			videoStr = &v
		}
		if len(audiosJSON) > 0 && string(audiosJSON) != "null" {
			v := string(audiosJSON)
			audiosStr = &v
		}

		isFree := int64(0)
		if p.IsFree {
			isFree = 1
		}

		if _, err := stmt.ExecContext(ctx, p.ID, int64(p.EventID), int64(p.ServiceID), int64(p.NetworkID), p.StartAt, int64(p.Duration), isFree, name, desc, genresStr, videoStr, audiosStr); err != nil {
			return fmt.Errorf("upsert program %d: %w", p.ID, err)
		}
	}

	return tx.Commit()
}

func (s *sqliteStore) Get(ctx context.Context, id int64) (*Program, bool, error) {
	p, err := s.q.GetProgram(ctx, id)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	prog, err := fromStoregenProgram(p)
	if err != nil {
		return nil, false, err
	}
	return prog, true, nil
}

func (s *sqliteStore) List(ctx context.Context, query Query) ([]*Program, error) {
	q := "SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE 1=1"
	args := make([]any, 0)

	if query.ID != nil {
		q += " AND id = ?"
		args = append(args, *query.ID)
	}
	if query.NetworkID != nil {
		q += " AND network_id = ?"
		args = append(args, int64(*query.NetworkID))
	}
	if query.ServiceID != nil {
		q += " AND service_id = ?"
		args = append(args, int64(*query.ServiceID))
	}
	if query.EventID != nil {
		q += " AND event_id = ?"
		args = append(args, int64(*query.EventID))
	}

	q += " ORDER BY start_at, id"

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*Program
	for rows.Next() {
		p, err := scanProgram(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *sqliteStore) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	return s.q.DeleteEndedAtBefore(ctx, cutoff)
}

func scanProgram(row interface{ Scan(dest ...any) error }) (*Program, error) {
	var p Program
	var eventID, serviceID, networkID, duration int64
	var isFree int64
	var name, desc, genresStr, videoStr, audiosStr *string

	if err := row.Scan(&p.ID, &eventID, &serviceID, &networkID, &p.StartAt, &duration, &isFree, &name, &desc, &genresStr, &videoStr, &audiosStr); err != nil {
		return nil, err
	}

	p.EventID = uint16(eventID)
	p.ServiceID = uint16(serviceID)
	p.NetworkID = uint16(networkID)
	p.Duration = int(duration)
	p.IsFree = isFree != 0
	if name != nil {
		p.Name = *name
	}
	if desc != nil {
		p.Description = *desc
	}
	if genresStr != nil {
		if err := decodeProgramJSON(p.ID, "genres", genresStr, &p.Genres); err != nil {
			return nil, err
		}
	}
	if videoStr != nil {
		if err := decodeProgramJSON(p.ID, "video", videoStr, &p.Video); err != nil {
			return nil, err
		}
	}
	if audiosStr != nil {
		if err := decodeProgramJSON(p.ID, "audios", audiosStr, &p.Audios); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func fromStoregenProgram(p gen.Program) (*Program, error) {
	prog := &Program{
		ID:        p.ID,
		EventID:   uint16(p.EventID),
		ServiceID: uint16(p.ServiceID),
		NetworkID: uint16(p.NetworkID),
		StartAt:   p.StartAt,
		Duration:  int(p.Duration),
		IsFree:    p.IsFree != 0,
	}
	if p.Name != nil {
		prog.Name = *p.Name
	}
	if p.Description != nil {
		prog.Description = *p.Description
	}
	if p.Genres != nil {
		if err := decodeProgramJSON(prog.ID, "genres", p.Genres, &prog.Genres); err != nil {
			return nil, err
		}
	}
	if p.Video != nil {
		if err := decodeProgramJSON(prog.ID, "video", p.Video, &prog.Video); err != nil {
			return nil, err
		}
	}
	if p.Audios != nil {
		if err := decodeProgramJSON(prog.ID, "audios", p.Audios, &prog.Audios); err != nil {
			return nil, err
		}
	}
	return prog, nil
}

func decodeProgramJSON(id int64, field string, value *string, target any) error {
	if err := json.Unmarshal([]byte(*value), target); err != nil {
		return fmt.Errorf("decode program %d %s: %w", id, field, err)
	}
	return nil
}
