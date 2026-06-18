-- name: ListServices :many
SELECT * FROM services;

-- name: GetServiceByID :one
SELECT * FROM services WHERE id = ?;

-- name: GetServicesByChannel :many
SELECT * FROM services WHERE channel_type = ? AND channel_id = ?;

-- name: DeleteServicesByChannel :exec
DELETE FROM services WHERE channel_type = ? AND channel_id = ?;
