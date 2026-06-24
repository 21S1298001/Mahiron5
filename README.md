# mahiron

Mahiron written in Go.

## Development

```sh
go run ./cmd/mahiron
go build ./cmd/mahiron
go generate ./internal/web/api
go tool sqlc generate
GOCACHE=/private/tmp/mahiron-gocache go test ./...
make test-race
make verify
```
