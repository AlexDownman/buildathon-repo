#!/bin/sh
# Probe build (not a real entry). Times the pipeline and some machine
# primitives on a generated 20M-event set, then runs it on /data with the
# results added as extra campaign_daily rows. The dev check fails on purpose,
# and the differing rows it reports carry the measurements.
# Rows: 0 machine info, 1 micro-benchmarks, then pipeline runs with
# 8, 8, 8 without huge pages, and 16 threads.
set -u
B=/app/shopping-sessions
R=/scratch/runs
N=/scratch/notes
now() { date +%s.%N; }
s=$(now)
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1
e=$(now)
g=$(awk "BEGIN{printf \"%.1f\", $e-$s}")
PROBE_OUT=$R PROBE_MICRO=/scratch/g $B
run() {
  env "$@" PROBE_OUT=$R DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o $B 2>&1 | tail -1 >&2
  rm -rf /scratch/o
}
run NCPU=8
run NCPU=8
run NCPU=8 NOHUGE=1
run NCPU=16
thp=$(sed 's/.*\[\(.*\)\].*/\1/' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)
cpu=$(grep -m1 "model name" /proc/cpuinfo | sed 's/.*: //' | cut -c1-36)
echo "n$(nproc) k$(uname -r | cut -c1-20) thp=$thp gen=${g}s $cpu" > $N
cat $R >> $N
cat $N >&2
PROBE_NOTES=$N $B
