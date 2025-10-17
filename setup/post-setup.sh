#!/bin/bash

newgrp docker

docker run hello-world

echo export "PATH=/usr/local/go/bin:$PATH" >> ~/.bashrc