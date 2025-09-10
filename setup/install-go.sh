#!/bin/bash

# Install Go 1.23.1 for container-bench project
# This script will install the exact Go version needed for the project

set -e

GO_VERSION="1.23.1"
GO_TARBALL="go${GO_VERSION}.linux-amd64.tar.gz"
GO_URL="https://go.dev/dl/${GO_TARBALL}"
INSTALL_DIR="/usr/local"

echo "=== Installing Go ${GO_VERSION} ==="

# Check if running as root for installation
if [[ $EUID -ne 0 ]]; then
   echo "This script needs to be run with sudo privileges for system-wide installation."
   echo "Usage: sudo ./setup/install-go.sh"
   exit 1
fi

# Download Go
echo "Downloading Go ${GO_VERSION}..."
cd /tmp
wget "${GO_URL}"

# Remove existing Go installation
echo "Removing existing Go installation..."
rm -rf "${INSTALL_DIR}/go"

# Extract new Go installation
echo "Installing Go ${GO_VERSION} to ${INSTALL_DIR}..."
tar -C "${INSTALL_DIR}" -xzf "${GO_TARBALL}"

# Clean up
echo "Cleaning up..."
rm -f "${GO_TARBALL}"

echo "Go ${GO_VERSION} installed successfully!"
echo ""
echo "To use Go, add the following to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
echo "export PATH=/usr/local/go/bin:\$PATH"
echo ""
echo "Or run the following command in your current session:"
echo "export PATH=/usr/local/go/bin:\$PATH"
echo ""
echo "Verify installation with: go version"
