#!/bin/bash

# Environment setup script for container-bench

echo "Setting up container-bench environment..."

# Check if .env file exists
if [ -f .env ]; then
    echo "✅ .env file found - environment variables will be loaded automatically"
    echo "Environment variables in .env file:"
    grep -v '^#' .env | grep -v '^$' | cut -d= -f1 | sed 's/^/  - /'
else
    echo "⚠️  No .env file found. Please create one with your InfluxDB configuration."
    echo "Example .env file:"
    cat << EOF
INFLUXDB_HOST=https://your-influxdb-host
INFLUXDB_USER=your-username
INFLUXDB_TOKEN=your-token
INFLUXDB_ORG=your-org
INFLUXDB_BUCKET=benchmarks
EOF
    echo ""
    echo "The application will automatically load variables from .env file."
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
echo "Note: Environment variables are automatically loaded from .env file"
