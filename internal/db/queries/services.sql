-- name: ListServices :many
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id;

-- name: GetServiceByID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.id = ?;

-- name: GetServiceByItemID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.network_id * 100000 + s.service_id = ?;

-- name: GetServiceByNetworkServiceID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.network_id = ? AND s.service_id = ?;

-- name: GetServicesByChannel :many
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = ? AND s.channel_id = ?;

-- name: GetServiceByChannelAndID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = sqlc.arg(channel_type)
  AND s.channel_id = sqlc.arg(channel_id)
  AND s.id = sqlc.arg(id)
UNION ALL
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = sqlc.arg(channel_type)
  AND s.channel_id = sqlc.arg(channel_id)
  AND s.id != sqlc.arg(id)
  AND s.network_id * 100000 + s.service_id = sqlc.arg(item_id)
LIMIT 1;

-- name: CountServices :one
SELECT COUNT(*) FROM services;

-- name: GetEPGSummary :one
SELECT COUNT(CASE
         WHEN epg.last_success_at IS NULL
           OR sqlc.arg(now) - epg.last_success_at > sqlc.arg(stale_after)
         THEN 1
       END) AS stale,
       COUNT(CASE
         WHEN epg.last_error IS NOT NULL AND epg.last_error != ''
         THEN 1
       END) AS failed,
       MAX(epg.last_success_at) AS last_success_at
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id;

-- name: DeleteServicesByChannel :exec
DELETE FROM services WHERE channel_type = ? AND channel_id = ?;

-- name: UpsertService :exec
INSERT INTO services (id, service_id, network_id, transport_stream_id, name, type, remote_control_key_id, channel_type, channel_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  transport_stream_id=excluded.transport_stream_id,
  name=excluded.name,
  type=excluded.type,
  remote_control_key_id=excluded.remote_control_key_id,
  channel_type=excluded.channel_type,
  channel_id=excluded.channel_id;

-- name: SetEPGAttempt :exec
INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_error)
VALUES (?, ?, ?, ?)
ON CONFLICT(network_id, service_id) DO UPDATE SET
  last_attempt_at=excluded.last_attempt_at,
  last_error=excluded.last_error;

-- name: SetEPGSuccess :exec
INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_success_at, last_error)
VALUES (?, ?, ?, ?, NULL)
ON CONFLICT(network_id, service_id) DO UPDATE SET
  last_attempt_at=excluded.last_attempt_at,
  last_success_at=excluded.last_success_at,
  last_error=NULL;
