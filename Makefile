test:
	go test ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

build:
	go build ./...

.PHONY: test lint fmt build
