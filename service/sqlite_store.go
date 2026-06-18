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
	return fromStoregenService(svc), nil
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
	return result, nil
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
