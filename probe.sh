#!/bin/sh
# Probe build (not a real entry). Times pipeline variants on a generated
# 20M-event set and once on /data, then runs it on /data with the results
# added as extra campaign_daily rows. The dev check fails on purpose, and the
# differing rows it reports carry the measurements, two per row:
#   0: machine info | the real /data (base settings)
#   1: base | mmap input          2: no huge pages | 4 threads
#   3: 12 threads | evbits10+pbits11   4: mmap+no huge pages | base again
# Per run: phase wall seconds (L links, P parse, D dedup, S sessions,
# W write; P and S as wall/user/sys), total wall, user and sys CPU seconds.
set -u
B=/app/shopping-sessions
R=/scratch/runs
N=/scratch/notes
t0=$(date +%s.%N)
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1
t1=$(date +%s.%N)
th=/sys/kernel/mm/transparent_hugepage
sz() { du -sm "$1" 2>/dev/null | cut -f1; }
cnt() { ls "$1" 2>/dev/null | wc -l; }
echo "I n$(nproc) sib=$(cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list 2>/dev/null) \
mem=$(awk '/MemTotal/{print int($2/1048576)}' /proc/meminfo)G \
thp=$(sed 's/.*\[\(.*\)\].*/\1/' $th/enabled) def=$(sed 's/.*\[\(.*\)\].*/\1/' $th/defrag) \
a512=$(grep -c -m1 avx512f /proc/cpuinfo) gen=$(awk "BEGIN{printf \"%.1f\", $t1 - $t0}") \
d:e$(sz /data/events)M/$(cnt /data/events) c$(sz /data/clicks)M/$(cnt /data/clicks) l$(sz /data/links)M/$(cnt /data/links) \
g:e$(sz /scratch/g/events)M" > $R
run() {
  env "$@" NCPU=${NC:-8} PROBE_OUT=$R DATA_DIR=${DD:-/scratch/g} OUTPUT_DIR=/scratch/o $B 2>&1 | tail -1 >&2
  rm -rf /scratch/o
}
DD=/data run PROBE_TAG=data
run PROBE_TAG=base
run PROBE_TAG=mmap MMAPIO=1
run PROBE_TAG=nohuge NOHUGE=1
NC=4 run PROBE_TAG=t4
NC=12 run PROBE_TAG=t12
run PROBE_TAG=eb10pb11 EVBITS=10 PBITS=11
run PROBE_TAG=mmapnh MMAPIO=1 NOHUGE=1
run PROBE_TAG=base
paste -d'|' - - < $R > $N
cat $N >&2
PROBE_NOTES=$N $B
