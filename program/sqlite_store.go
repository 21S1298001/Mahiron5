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

	if err := upsertPrograms(ctx, tx, programs); err != nil {
		return err
	}

	return tx.Commit()
}

func upsertPrograms(ctx context.Context, tx *sql.Tx, programs []*Program) error {
	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO programs (id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios, extended, related_items, series) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
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

		genresStr, err := encodeProgramJSON(p.ID, "genres", p.Genres)
		if err != nil {
			return err
		}
		videoStr, err := encodeProgramJSON(p.ID, "video", p.Video)
		if err != nil {
			return err
		}
		audiosStr, err := encodeProgramJSON(p.ID, "audios", p.Audios)
		if err != nil {
			return err
		}
		extendedStr, err := encodeProgramJSON(p.ID, "extended", p.Extended)
		if err != nil {
			return err
		}
		relatedStr, err := encodeProgramJSON(p.ID, "related_items", p.RelatedItems)
		if err != nil {
			return err
		}
		seriesStr, err := encodeProgramJSON(p.ID, "series", p.Series)
		if err != nil {
			return err
		}

		isFree := int64(0)
		if p.IsFree {
			isFree = 1
		}

		if _, err := stmt.ExecContext(ctx, p.ID, int64(p.EventID), int64(p.ServiceID), int64(p.NetworkID), p.StartAt, int64(p.Duration), isFree, name, desc, genresStr, videoStr, audiosStr, extendedStr, relatedStr, seriesStr); err != nil {
			return fmt.Errorf("upsert program %d: %w", p.ID, err)
		}
	}

	return nil
}

func (s *sqliteStore) Get(ctx context.Context, id int64) (*Program, bool, error) {
	row := s.db.QueryRowContext(ctx, programSelect+" WHERE id = ?", id)
	p, err := scanProgram(row)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return p, true, nil
}

func (s *sqliteStore) List(ctx context.Context, query Query) ([]*Program, error) {
	q := programSelect + " WHERE 1=1"
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

func (s *sqliteStore) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM programs WHERE network_id = ? AND service_id = ? AND start_at + duration >= ?`, networkID, serviceID, from); err != nil {
		return fmt.Errorf("delete service snapshot: %w", err)
	}
	if err := upsertPrograms(ctx, tx, programs); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *sqliteStore) Count(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM programs").Scan(&count)
	return count, err
}

const programSelect = `SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios, extended, related_items, series FROM programs`

func scanProgram(row interface{ Scan(dest ...any) error }) (*Program, error) {
	var p Program
	var eventID, serviceID, networkID, duration int64
	var isFree int64
	var name, desc, genresStr, videoStr, audiosStr, extendedStr, relatedStr, seriesStr *string

	if err := row.Scan(&p.ID, &eventID, &serviceID, &networkID, &p.StartAt, &duration, &isFree, &name, &desc, &genresStr, &videoStr, &audiosStr, &extendedStr, &relatedStr, &seriesStr); err != nil {
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
	if extendedStr != nil {
		if err := decodeProgramJSON(p.ID, "extended", extendedStr, &p.Extended); err != nil {
			return nil, err
		}
	}
	if relatedStr != nil {
		if err := decodeProgramJSON(p.ID, "related_items", relatedStr, &p.RelatedItems); err != nil {
			return nil, err
		}
	}
	if seriesStr != nil {
		if err := decodeProgramJSON(p.ID, "series", seriesStr, &p.Series); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func encodeProgramJSON(id int64, field string, value any) (*string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal program %d %s: %w", id, field, err)
	}
	if string(data) == "null" || string(data) == "[]" || string(data) == "{}" {
		return nil, nil
	}
	v := string(data)
	return &v, nil
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
