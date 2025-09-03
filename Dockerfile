# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o container-bench .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add \
    ca-certificates \
    docker-cli \
    linux-perf \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/container-bench .

# Copy example configurations
COPY examples/ ./examples/

# Create non-root user
RUN addgroup -g 1001 -S container-bench && \
    adduser -S container-bench -u 1001 -G container-bench

# Note: The container needs to run with --privileged flag to access RDT and perf
# USER container-bench

ENTRYPOINT ["./container-bench"]
