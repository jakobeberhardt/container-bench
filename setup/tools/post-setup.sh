#!/bin/bash
echo "Changing ownership of container-bench folder to $USER..."
sudo chown -R $USER: .

sudo usermod -aG docker $USER

echo ""
echo "To use Go, add the following to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
echo "export PATH=/usr/local/go/bin:\$PATH"
echo ""
echo "Or run the following command in your current session:"
echo "echo 'export PATH=/usr/local/go/bin:$PATH' >> $HOME/.bashrc"
echo ""
echo "Verify installation with: go version"
