# Mahiron5

Mahiron written in Go.

## Development

```sh
go run ./cmd/mahiron5
go build ./cmd/mahiron5
go generate ./internal/web/api
go tool sqlc generate
GOCACHE=/private/tmp/mahiron5-gocache go test ./...
```
