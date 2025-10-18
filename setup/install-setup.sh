#!/bin/bash

sudo bash setup/tools/update.sh
sudo bash setup/tools/install-docker.sh
sudo bash setup/tools/install-collectors.sh
sudo bash setup/tools/install-go.sh
sudo bash setup/tools/post-setup.sh