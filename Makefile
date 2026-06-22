.PHONY: build generate test test-race verify

build:
	go build ./cmd/mahiron5

generate:
	go generate ./internal/web/api
	go tool sqlc generate

test:
	GOCACHE=/private/tmp/mahiron5-gocache go test ./...

test-race:
	GOCACHE=/private/tmp/mahiron5-gocache go test -race ./internal/job ./internal/stream ./internal/tuner ./internal/util

verify: test test-race
