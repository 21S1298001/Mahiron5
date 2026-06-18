package service

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/21S1298001/Mahiron5/db/gen"
)

type sqliteStore struct {
	db *sql.DB
	q  *gen.Queries
}

func NewSQLiteStore(db *sql.DB) Store {
	return &sqliteStore{
		db: db,
		q:  gen.New(db),
	}
}

func (s *sqliteStore) List(ctx context.Context) ([]*Service, error) {
	svcs, err := s.q.ListServices(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*Service, len(svcs))
	for i := range svcs {
		result[i] = fromStoregenService(svcs[i])
	}
	if err := s.attachEPGStatuses(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sqliteStore) GetByID(ctx context.Context, id string) (*Service, error) {
	svc, err := s.q.GetServiceByID(ctx, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := fromStoregenService(svc)
	if err := s.attachEPGStatuses(ctx, []*Service{result}); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sqliteStore) GetByChannel(ctx context.Context, channelType, channelId string) ([]*Service, error) {
	svcs, err := s.q.GetServicesByChannel(ctx, gen.GetServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	})
	if err != nil {
		return nil, err
	}
	result := make([]*Service, len(svcs))
	for i := range svcs {
		result[i] = fromStoregenService(svcs[i])
	}
	if err := s.attachEPGStatuses(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *sqliteStore) SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_error)
		VALUES (?, ?, ?, ?) ON CONFLICT(network_id, service_id) DO UPDATE SET last_attempt_at=excluded.last_attempt_at, last_error=excluded.last_error`,
		networkID, serviceID, attemptedAt, nullableString(lastError))
	return err
}

func (s *sqliteStore) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_success_at, last_error)
		VALUES (?, ?, ?, ?, NULL) ON CONFLICT(network_id, service_id) DO UPDATE SET last_attempt_at=excluded.last_attempt_at, last_success_at=excluded.last_success_at, last_error=NULL`,
		networkID, serviceID, succeededAt, succeededAt)
	return err
}

func (s *sqliteStore) attachEPGStatuses(ctx context.Context, services []*Service) error {
	if len(services) == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT network_id, service_id, last_attempt_at, last_success_at, last_error FROM epg_service_status`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type key struct{ networkID, serviceID uint16 }
	byKey := make(map[key]*Service, len(services))
	for _, svc := range services {
		byKey[key{svc.NetworkId, svc.ServiceId}] = svc
	}
	for rows.Next() {
		var nid, sid int64
		var attempted, succeeded *int64
		var lastError *string
		if err := rows.Scan(&nid, &sid, &attempted, &succeeded, &lastError); err != nil {
			return err
		}
		if svc := byKey[key{uint16(nid), uint16(sid)}]; svc != nil {
			svc.EPG.LastAttemptAt, svc.EPG.LastSuccessAt = attempted, succeeded
			if lastError != nil {
				svc.EPG.LastError = *lastError
			}
		}
	}
	return rows.Err()
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *sqliteStore) ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	q := s.q.WithTx(tx)
	if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{
		ChannelType: channelType,
		ChannelID:   channelId,
	}); err != nil {
		return fmt.Errorf("delete existing: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO services (id, service_id, network_id, transport_stream_id, name, type, remote_control_key_id, channel_type, channel_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET service_id=excluded.service_id, network_id=excluded.network_id,
		transport_stream_id=excluded.transport_stream_id, name=excluded.name, type=excluded.type,
		remote_control_key_id=excluded.remote_control_key_id, channel_type=excluded.channel_type,
		channel_id=excluded.channel_id`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, svc := range services {
		if _, err := stmt.ExecContext(ctx, svc.Id, int64(svc.ServiceId), int64(svc.NetworkId), int64(svc.TransportStreamId), svc.Name, int64(svc.Type), int64(svc.RemoteControlKeyId), channelType, channelId); err != nil {
			return fmt.Errorf("insert service %s: %w", svc.Id, err)
		}
	}

	return tx.Commit()
}

func (s *sqliteStore) PruneChannels(ctx context.Context, active []ChannelKey) error {
	allowed := make(map[ChannelKey]struct{}, len(active))
	for _, key := range active {
		allowed[key] = struct{}{}
	}
	services, err := s.q.ListServices(ctx)
	if err != nil {
		return fmt.Errorf("list services: %w", err)
	}
	stale := make(map[ChannelKey]struct{})
	for _, svc := range services {
		key := ChannelKey{Type: svc.ChannelType, ID: svc.ChannelID}
		if _, ok := allowed[key]; !ok {
			stale[key] = struct{}{}
		}
	}
	if len(stale) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prune tx: %w", err)
	}
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	for key := range stale {
		if err := q.DeleteServicesByChannel(ctx, gen.DeleteServicesByChannelParams{ChannelType: key.Type, ChannelID: key.ID}); err != nil {
			return fmt.Errorf("delete stale channel %s/%s: %w", key.Type, key.ID, err)
		}
	}
	return tx.Commit()
}

func fromStoregenService(s gen.Service) *Service {
	return &Service{
		Id:                 s.ID,
		ServiceId:          uint16(s.ServiceID),
		NetworkId:          uint16(s.NetworkID),
		TransportStreamId:  uint16(s.TransportStreamID),
		Name:               s.Name,
		Type:               uint8(s.Type),
		RemoteControlKeyId: uint8(s.RemoteControlKeyID),
		ChannelType:        s.ChannelType,
		ChannelId:          s.ChannelID,
	}
}
