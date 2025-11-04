#!/bin/bash
sudo chown -R $USER: .
# Add user to docker group to run docker without sudo
sudo usermod -aG docker $USER

# Add Go to PATH
echo 'export PATH=/usr/local/go/bin:$PATH' >> $HOME/.bashrc
echo ""
echo "To use Go, add the following to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
echo "export PATH=/usr/local/go/bin:\$PATH"
echo ""
echo "Or run the following command in your current CloudLab session:"
echo "echo 'export PATH=/usr/local/go/bin:\$PATH' >> \$HOME/.bashrc && source \$HOME/.bashrc"
echo ""
echo "Verify installation with: go version"
