#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

JAR="${TLA2TOOLS_JAR:-$SCRIPT_DIR/tla2tools.jar}"

if [[ ! -f "$JAR" ]]; then
  echo "Downloading tla2tools.jar..."
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
fi

run_one() {
  local name="$1"
  local cfg="configs/${name}.cfg"
  echo "=========================================="
  echo "Scenario: $name ($cfg)"
  echo "=========================================="
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
    -deadlock -workers auto -cleanup \
    -config "$cfg" HTTP2Client.tla
  local rc=$?
  echo "-> exit code: $rc"
  echo ""
  return $rc
}

if [[ $# -gt 0 ]]; then
  for arg in "$@"; do
    run_one "${arg%.cfg}"
  done
  exit 0
fi

SCENARIOS=(
  bug1_master
  bug2_local_flag
  verify_pr9616_m1
  verify_pr9616_m2
  verify_pr9616_3callers
  edge_busyspin
  edge_streamid
  verify_full_fix
)

for s in "${SCENARIOS[@]}"; do
  run_one "$s" || true
done
