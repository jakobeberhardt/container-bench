#!/bin/bash
git clone https://github.com/jakobeberhardt/container-bench.git
cd ./container-bench
git checkout refactor

sudo bash setup/install-setup.sh
make 