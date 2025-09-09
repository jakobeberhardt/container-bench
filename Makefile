BINARY_NAME=container-bench
GO_FILES=$(shell find . -name "*.go" -type f)

.PHONY: all build clean test fmt vet deps

all: build

build: deps
	go build -o $(BINARY_NAME) ./cmd

clean:
	go clean
	rm -f $(BINARY_NAME)

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

deps:
	go mod tidy
	go mod download

install: build
	sudo cp $(BINARY_NAME) /usr/local/bin/

lint:
	golangci-lint run

# Development targets
dev-setup:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

run-example: build
	./$(BINARY_NAME) run -c examples/simple_test.yml

validate-example: build
	./$(BINARY_NAME) validate -c examples/simple_test.yml
