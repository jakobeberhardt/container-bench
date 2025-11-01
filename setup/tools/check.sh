#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <container_name_or_id>"
  exit 1
fi


CONTAINER="$1"
DURATION="${2:-5}"
CID=$(docker inspect --format '{{.Id}}' "$CONTAINER" 2>/dev/null)

if [ -z "$CID" ]; then
  echo "Error: container '$CONTAINER' not found."
  exit 1
fi

CGROUP="/system.slice/docker-${CID}.scope"
OUT=$(mktemp)

perf stat -x, -a \
  -e cycles,instructions,inst_retired.any,cycle_activity.stalls_total \
  --cgroup="$CGROUP" sleep ${DURATION} 2> "$OUT"

cycles=$(awk -F, '$3=="cycles"{gsub(/,/,"",$1); print $1}' "$OUT")
inst=$(awk -F, '$3=="inst_retired.any"{gsub(/,/,"",$1); print $1}' "$OUT")
stalls=$(awk -F, '$3=="cycle_activity.stalls_total"{gsub(/,/,"",$1); print $1}' "$OUT")

if [[ -z "$cycles" || -z "$inst" || -z "$stalls" ]]; then
  echo "Error: missing perf counters."
  cat "$OUT"
  rm -f "$OUT"
  exit 1
fi

useful=$(echo "$cycles - $stalls" | bc)
ipc=$(echo "scale=6; $inst / $cycles" | bc -l)
ipc_clean=$(echo "scale=6; $inst / $useful" | bc -l)
percent_stall=$(echo "scale=6; 100 * $stalls / $cycles" | bc -l)

echo "container=$CONTAINER"
echo "cgroup=$CGROUP"
echo "cycles=$cycles"
echo "inst_retired.any=$inst"
echo "stalled_cycles=$stalls"
echo "useful_cycles=$useful"
echo "ipc=$ipc"
echo "IPC_clean=$ipc_clean"
echo "percent_stalled=${percent_stall}%"

rm -f "$OUT"
