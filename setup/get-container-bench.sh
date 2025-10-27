#!/bin/bash
git clone https://github.com/jakobeberhardt/container-bench.git
cd ./container-bench
git checkout refactor

sudo bash setup/install-setup.sh
echo "Run "make" or "make" install"
echo "Validate installation with e.g. "container-bench validate --config examples/simple_test.yml""
