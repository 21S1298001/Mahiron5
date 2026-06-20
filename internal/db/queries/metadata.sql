-- name: GetMetadataValue :one
SELECT value FROM metadata WHERE key = ?;

-- name: UpsertMetadata :exec
INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?);
