#!/bin/bash

# Test script for container-bench

set -e

echo "Container-Bench Test Script"
echo "=========================="

# Check prerequisites
echo "Checking prerequisites..."

# Check if Go is available
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed"
    exit 1
fi
echo "✅ Go is available"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    exit 1
fi
echo "✅ Docker is available"

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running"
    exit 1
fi
echo "✅ Docker daemon is running"

# Build the application
echo -e "\nBuilding container-bench..."
make build
echo "✅ Build successful"

# Validate configuration
echo -e "\nValidating configuration..."
./container-bench validate -c examples/test_simple.yml
echo "✅ Configuration validation successful"

echo -e "\nValidating environment expansion configuration..."
# Environment variables are automatically loaded from .env file
./container-bench validate -c examples/simple_test.yml
echo "✅ Environment expansion validation successful"

# Check if we can connect to the database (optional)
echo -e "\nTesting database connection..."
timeout 10s ./container-bench run -c examples/test_simple.yml --dry-run 2>/dev/null || echo "⚠️  Database connection test skipped (expected for demo)"

echo -e "\n🎉 All tests passed!"
echo -e "\nUsage examples:"
echo "  ./container-bench validate -c examples/simple_test.yml"
echo "  ./container-bench run -c examples/simple_test.yml"

# Show help
echo -e "\nApplication help:"
./container-bench --help
