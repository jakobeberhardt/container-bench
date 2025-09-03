# Makefile for Container Bench

.PHONY: build clean test lint install examples docker-build docker-run help

# Build variables
BINARY_NAME=container-bench
BINARY_PATH=./$(BINARY_NAME)
BUILD_DIR=./build
VERSION=$(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.1.0")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Go variables
GOPATH=$(shell go env GOPATH)
GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)

# Ldflags for version information
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)"

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME) $(VERSION)..."
	@go build $(LDFLAGS) -o $(BINARY_PATH) .
	@echo "Build complete: $(BINARY_PATH)"

## clean: Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BINARY_PATH)
	@rm -rf $(BUILD_DIR)
	@go clean

## test: Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

## lint: Run linter
lint:
	@echo "Running linter..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

## format: Format code
format:
	@echo "Formatting code..."
	@go fmt ./...
	@if command -v goimports > /dev/null; then \
		goimports -w .; \
	fi

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

## install: Install the binary to system
install: build
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	@sudo cp $(BINARY_PATH) /usr/local/bin/
	@echo "Installation complete"

## build-all: Build for multiple platforms
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 .
	@GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 .
	@GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .
	@GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .
	@echo "Multi-platform build complete in $(BUILD_DIR)/"

## examples: Run example configurations
examples: build
	@echo "Running example configurations..."
	@echo "1. Validating simple test..."
	@$(BINARY_PATH) validate -c examples/simple_test.yml
	@echo "2. Validating Redis/NGINX benchmark..."
	@$(BINARY_PATH) validate -c examples/redis_nginx_benchmark.yml
	@echo "3. Validating memory intensive benchmark..."
	@$(BINARY_PATH) validate -c examples/memory_intensive.yml
	@echo "All example configurations are valid!"

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t container-bench:$(VERSION) -t container-bench:latest .

## docker-run: Run in Docker container
docker-run: docker-build
	@echo "Running in Docker container..."
	@docker run --rm -it \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v /sys/fs/resctrl:/sys/fs/resctrl \
		-v $(PWD)/examples:/app/examples \
		--privileged \
		container-bench:latest validate -c examples/simple_test.yml

## setup-rdt: Setup Intel RDT (requires sudo)
setup-rdt:
	@echo "Setting up Intel RDT..."
	@sudo mkdir -p /sys/fs/resctrl
	@sudo mount -t resctrl resctrl /sys/fs/resctrl 2>/dev/null || echo "resctrl already mounted"
	@sudo chmod -R 755 /sys/fs/resctrl
	@echo "RDT setup complete"

## setup-perf: Setup perf events (requires sudo)
setup-perf:
	@echo "Setting up perf events..."
	@echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid > /dev/null
	@echo "Perf events setup complete"

## setup: Setup system for container-bench (requires sudo)
setup: setup-rdt setup-perf
	@echo "System setup complete"

## benchmark: Run a quick benchmark test
benchmark: build
	@echo "Running quick benchmark test..."
	@$(BINARY_PATH) validate -c examples/simple_test.yml
	@echo "Benchmark validation successful!"

## version: Show version information
version:
	@echo "Container Bench"
	@echo "Version: $(VERSION)"
	@echo "Commit: $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(shell go version)"

## help: Show this help
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

# Default target
.DEFAULT_GOAL := build
