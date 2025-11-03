#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <container_name_or_id> [duration_sec]"
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
  -e cycles,instructions,inst_retired.any,cycle_activity.stalls_total,cycle_activity.stalls_l1d_miss,cycle_activity.stalls_l2_miss,cycle_activity.stalls_l3_miss,resource_stalls.sb,resource_stalls.scoreboard \
  --cgroup="$CGROUP" sleep "$DURATION" 2> "$OUT"

get_val() {
  awk -F, -v name="$1" '$3==name{gsub(/,/,"",$1); print $1}' "$OUT"
}

cycles=$(get_val "cycles")
inst=$(get_val "inst_retired.any")
stalls_total=$(get_val "cycle_activity.stalls_total")
stalls_l1d=$(get_val "cycle_activity.stalls_l1d_miss")
stalls_l2=$(get_val "cycle_activity.stalls_l2_miss")
stalls_l3=$(get_val "cycle_activity.stalls_l3_miss")
stalls_sb=$(get_val "resource_stalls.sb")
stalls_scoreboard=$(get_val "resource_stalls.scoreboard")

if [[ -z "$cycles" || -z "$inst" || -z "$stalls_total" ]]; then
  echo "Error: missing perf counters."
  cat "$OUT"
  rm -f "$OUT"
  exit 1
fi

useful=$(echo "$cycles - $stalls_total" | bc)
ipc=$(echo "scale=6; $inst / $cycles" | bc -l)
ipc_clean=$(echo "scale=6; $inst / $useful" | bc -l)
percent_stall=$(echo "scale=6; 100 * $stalls_total / $cycles" | bc -l)

echo "container=$CONTAINER"
echo "cgroup=$CGROUP"
echo "duration=${DURATION}s"
echo "cycles=$cycles"
echo "inst_retired.any=$inst"
echo "stalled_cycles_total=$stalls_total"
echo "stalled_cycles_l1d_miss=$stalls_l1d"
echo "stalled_cycles_l2_miss=$stalls_l2"
echo "stalled_cycles_l3_miss=$stalls_l3"
echo "store_buffer_stalls=$stalls_sb"
echo "scoreboard_stalls=$stalls_scoreboard"
echo "useful_cycles=$useful"
echo "ipc=$ipc"
echo "IPC_clean=$ipc_clean"
echo "percent_stalled=${percent_stall}%"

rm -f "$OUT"
