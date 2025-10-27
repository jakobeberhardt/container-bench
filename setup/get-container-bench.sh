#!/bin/bash
git clone https://github.com/jakobeberhardt/container-bench.git
cd ./container-bench
sudo bash setup/install-setup.sh

echo "Run "make" or "make install""
echo "and run e.g."
echo ""
echo 'sudo container-bench validate -c examples/simple_test.yml'
echo 'sudo container-bench run -c examples/simple_test.yml'