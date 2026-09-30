.PHONY: run fmt fmt-check test test-race vet build module-check check check-ci

run:
	go run ./cmd/worker

fmt:
	gofmt -w $$(find . -type f -name '*.go' -not -path './.git/*' -not -path './vendor/*')

fmt-check:
	test -z "$$(gofmt -l $$(find . -type f -name '*.go' -not -path './.git/*' -not -path './vendor/*'))"

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build -o bin/worker ./cmd/worker

module-check:
	@set -e; modules=$$(go list -deps -f '{{if .Module}}{{.Module.Path}}{{end}}' ./...); \
	if printf '%s\n' "$$modules" | grep -E '^stack-atlas-(api|web)$$'; then \
		echo 'Engine may not depend on the API or Web Go modules.'; exit 1; \
	fi

check: fmt-check test test-race vet build module-check

check-ci: check
