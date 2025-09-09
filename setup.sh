#!/bin/bash

# Environment setup script for container-bench

echo "Setting up container-bench environment..."

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
    echo "✅ Environment variables loaded"
else
    echo "⚠️  No .env file found. Please create one with your InfluxDB configuration."
    echo "Example .env file:"
    echo "INFLUXDB_HOST=https://your-influxdb-host"
    echo "INFLUXDB_USER=your-username"
    echo "INFLUXDB_TOKEN=your-token"
    echo "INFLUXDB_ORG=your-org"
    echo "INFLUXDB_BUCKET=benchmarks"
fi

# Verify Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker first."
    exit 1
fi
echo "✅ Docker is running"

# Build the application if needed
if [ ! -f container-bench ]; then
    echo "Building container-bench..."
    make build
fi
echo "✅ container-bench is ready"

echo ""
echo "🎉 Environment setup complete!"
echo ""
echo "Available commands:"
echo "  ./container-bench validate -c examples/simple_test.yml"
echo "  ./container-bench run -c examples/simple_test.yml"
echo ""
echo "Environment variables:"
env | grep INFLUXDB | head -5
