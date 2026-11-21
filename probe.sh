#!/bin/sh
# Probe build (not a real entry): runs the normal pipeline on /data, then times
# it on a generated 20M-event dataset and prints everything to stderr. At the
# end it deletes campaign_daily on purpose, so the dev check fails and the
# scorer shows these logs.
set -u
B=/app/shopping-sessions
now() { date +%s.%N; }
$B
s=$(now)
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1
e=$(now)
echo "probe: generate took $(awk "BEGIN{print $e-$s}")s" >&2
{
  uname -a; nproc; grep -m1 "model name" /proc/cpuinfo; grep -m1 "cpu MHz" /proc/cpuinfo
  echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled) / $(cat /sys/kernel/mm/transparent_hugepage/defrag)"
  cat /sys/fs/cgroup/cpu.max 2>/dev/null; cat /sys/fs/cgroup/memory.max 2>/dev/null
  grep -E "MemTotal|MemFree|^Cached" /proc/meminfo; du -sh /scratch/g
} >&2
run() {
  echo "probe: run $*" >&2
  s=$(now)
  env "$@" DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o $B
  e=$(now)
  echo "probe: wall $(awk "BEGIN{print $e-$s}")s" >&2
  rm -rf /scratch/o
}
run NCPU=8
run NCPU=8
run NCPU=8
run NCPU=8 NOHUGE=1
run NCPU=16
run NCPU=4
run NCPU=1
rm -f /output/campaign_daily/*.parquet
echo "probe: removed campaign_daily on purpose" >&2
exit 0
