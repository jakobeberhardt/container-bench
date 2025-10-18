# Setup Scripts for container-bench

This directory contains setup scripts to prepare the system for building and running the container-bench application.

**What it does:**
- Runs the Go installation script (requires sudo)
- Configures the Go PATH in `~/.bashrc`
- Sets up the environment for the current session
- Downloads project dependencies
- Verifies the installation

## Quick Start

For a complete system setup, simply run:

```bash
sudo bash ./setup/install-setup.sh
```

## Manual Verification

After running the setup, you can verify everything is working:

```bash
# Check Go version (should show go1.24.7 due to toolchain)
go version

# Build the project
make build

# Run a simple test
sudo ./container-bench run -c examples/simple_test.yml
```
