-- name: GetProgram :one
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE id = ?;

-- name: ListProgramsByNetworkAndService :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE network_id = ? AND service_id = ?;

-- name: ListProgramsByNetwork :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE network_id = ?;

-- name: ListProgramsByService :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE service_id = ?;

-- name: ListProgramsByEvent :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs WHERE event_id = ?;

-- name: ListAllPrograms :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free, name, description, genres, video, audios FROM programs;

-- name: DeleteEndedAtBefore :exec
DELETE FROM programs WHERE start_at + duration < ?;

-- name: DeleteAllPrograms :exec
DELETE FROM programs;
