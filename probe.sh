#!/bin/sh
# Probe build (not a real entry). Measures the machine and times pipeline
# variants on a generated 20M-event set, then runs on /data with the results
# added as extra campaign_daily rows. The dev check fails on purpose, and the
# differing rows it reports carry the measurements, two per row:
#   0: machine info | thread scaling (S: alu/bw per thread count, cpus, cores)
#   1: base | mmap input      2: 6 threads | stage depth 4
#   3: 1MB pieces | kernel counters during a base run (V)
#   4: base again
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
q() { cat "$1" 2>/dev/null | tr '\n' ' ' | cut -c1-24; }
echo "I n$(nproc) gen=$(awk "BEGIN{printf \"%.1f\", $t1 - $t0}") \
cg=$(q /sys/fs/cgroup/cpu.max)/$(q /sys/fs/cgroup/cpuset.cpus.effective) \
mhz=$(awk -F: '/cpu MHz/{printf "%d", $2; exit}' /proc/cpuinfo) \
l3=$(awk -F: '/cache size/{print $2; exit}' /proc/cpuinfo | tr -d ' ') \
khp=$(q $th/khugepaged/defrag) shm=$(q $th/shmem_enabled)" > $R
PROBE_SCALE=1 PROBE_OUT=$R $B
run() {
  env "$@" NCPU=${NC:-8} PROBE_OUT=$R DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o $B 2>&1 | tail -1 >&2
  rm -rf /scratch/o
}
vm() { awk '/^(thp_fault_alloc|thp_fault_fallback|compact_stall|pgfault|pgmajfault|thp_split_pmd) /{n=$1; sub(/thp_fault_/,"t",n); sub(/compact_/,"c",n); sub(/pg/,"",n); sub(/thp_split_pmd/,"split",n); printf "%s=%s ", n, $2}' /proc/vmstat; }
run PROBE_TAG=base
run PROBE_TAG=mmap MMAPIO=1
NC=6 run PROBE_TAG=t6
run PROBE_TAG=k4 STAGE4=1
run PROBE_TAG=pc1024 PIECE_KB=1024
v0=$(vm)
run PROBE_TAG=base
v1=$(vm)
echo "V $v0 > $v1" >> $R
paste -d'|' - - < $R > $N
cat $N >&2
PROBE_NOTES=$N $B
