#!/bin/bash
# Insert + full-scan benchmark, 5 runs. Usage: ./scripts/bench.sh
cd "$(dirname "$0")/.." || exit 1
for i in 1 2 3 4 5; do
  rm -rf /tmp/bench_prof*
  cargo run --release -p mitrdb-server --example perf_bench -- /tmp/bench_prof.db /tmp/bench_prof.wal 10000 10 batch 1 4096 2>&1 | grep -E "^error|raw insert|full-scan"
done
