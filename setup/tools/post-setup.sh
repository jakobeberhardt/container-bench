#!/bin/bash
# Add user to docker group to run docker without sudo
sudo usermod -aG docker $USER

# Add Go to PATH
echo 'export PATH=/usr/local/go/bin:$PATH' >> $HOME/.bashrc