#!/bin/bash

# Complete system setup for container-bench project
# This script sets up Go and configures the environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Container-Bench System Setup ==="
echo "Project directory: $PROJECT_DIR"
echo ""

# Check if Go installation script exists
if [[ ! -f "$SCRIPT_DIR/install-go.sh" ]]; then
    echo "Error: install-go.sh not found in setup directory"
    exit 1
fi

# Install Go
echo "Installing Go..."
sudo "$SCRIPT_DIR/install-go.sh"

# Configure PATH for current user
echo "Configuring Go PATH for current user..."
GO_PATH_EXPORT='export PATH=/usr/local/go/bin:$PATH'

# Add to .bashrc if not already present
if ! grep -q "/usr/local/go/bin" ~/.bashrc; then
    echo "" >> ~/.bashrc
    echo "# Go programming language" >> ~/.bashrc
    echo "$GO_PATH_EXPORT" >> ~/.bashrc
    echo "Go PATH added to ~/.bashrc"
else
    echo "Go PATH already configured in ~/.bashrc"
fi

# Set PATH for current session
export PATH=/usr/local/go/bin:$PATH

# Verify Go installation
echo ""
echo "Verifying Go installation..."
go version

# Navigate to project directory and verify go.mod
echo ""
echo "Verifying project setup..."
cd "$PROJECT_DIR"

if [[ -f "go.mod" ]]; then
    echo "Found go.mod file"
    go mod download
    echo "Dependencies downloaded successfully"
else
    echo "Warning: go.mod not found in project directory"
fi

echo ""
echo "=== Setup Complete ==="
echo "Go has been installed and configured."
echo "To use Go in new terminal sessions, restart your terminal or run:"
echo "source ~/.bashrc"
echo ""
echo "To build the container-bench project, run:"
echo "cd $PROJECT_DIR"
echo "make build"
