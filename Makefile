.PHONY: run fmt test vet build

run:
	go run ./cmd/worker

fmt:
	gofmt -w $$(find cmd internal -type f -name '*.go')

test:
	go test ./...

vet:
	go vet ./...

build:
	go build -o bin/worker ./cmd/worker
