#!/bin/bash

docker build -t registry.jakob-eberhardt.de/neighbor:default .
docker push registry.jakob-eberhardt.de/neighbor:default
echo "Test the image locally:"
echo "docker run --rm registry.jakob-eberhardt.de/neighbor:default ./neighbor --duration 10 --buffer-size 50MB --pattern sequential"