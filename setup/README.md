# Setup Scripts for Container-Bench

This directory contains setup scripts to prepare the system for building and running the container-bench application.

## Scripts

### install-go.sh
Installs the exact Go version (1.23.1) required by the project.

**Usage:**
```bash
sudo ./setup/install-go.sh
```

**What it does:**
- Downloads Go 1.23.1 from the official Go website
- Removes any existing Go installation in `/usr/local/go`
- Installs Go to `/usr/local/go`
- Provides instructions for PATH configuration

### setup-system.sh
Complete system setup that installs Go and configures the environment.

**Usage:**
```bash
./setup/setup-system.sh
```

**What it does:**
- Runs the Go installation script (requires sudo)
- Configures the Go PATH in `~/.bashrc`
- Sets up the environment for the current session
- Downloads project dependencies
- Verifies the installation

## Quick Start

For a complete system setup, simply run:

```bash
./setup/setup-system.sh
```

This will handle everything needed to build and run the container-bench application.

## Manual Verification

After running the setup, you can verify everything is working:

```bash
# Check Go version (should show go1.24.7 due to toolchain)
go version

# Build the project
make build

# Run a simple test
sudo ./container-bench -c examples/simple_test.yml
```

## Requirements

- Linux x86_64 system
- sudo access for system-wide Go installation
- Internet connection for downloading Go and dependencies
