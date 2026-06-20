.PHONY: build generate test

build:
	go build ./cmd/mahiron5

generate:
	go generate ./internal/web/api
	go tool sqlc generate

test:
	GOCACHE=/private/tmp/mahiron5-gocache go test ./...
