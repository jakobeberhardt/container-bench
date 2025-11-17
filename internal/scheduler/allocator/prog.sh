#!/bin/bash

# Usage: ./script.sh <container-name>

NAME="$1"
if [ -z "$NAME" ]; then
    echo "container name required"
    exit 1
fi

CID=$(docker inspect "$NAME" | jq -r '.[0].Id')
if [ -z "$CID" ] || [ "$CID" = "null" ]; then
    echo "container not found"
    exit 1
fi

MAP=( \
0x001 0x003 0x007 0x00F 0x01F 0x03F \
0x07F 0x0FF 0x1FF 0x3FF 0x7FF 0xFFF )

for MASK in "${MAP[@]}"; do
    sudo runc --root /run/docker/runtime-runc/moby update \
        --l3-cache-schema "L3:0=${MASK}" "$CID"
    echo Mask applied: ${MASK}
    echo resctrl: 
    cat  /sys/fs/resctrl/${CID}/schemata
    sleep 10
done
