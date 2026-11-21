#!/bin/sh
# Probe build (not a real entry). Times pipeline variants on a generated
# 20M-event set, then runs it on /data with the results added as extra
# campaign_daily rows. The dev check fails on purpose, and the differing rows
# it reports carry the measurements, two runs per row:
#   0: T8 | T8m   1: T8 | T8m   2: T16 | T16m   3: T12m | T8mh   4: T6m | T4m
# (m = input via mmap, h = no huge pages). Per run: phase wall seconds
# (L links, P parse, D dedup, S sessions, W write; P and S as wall/user/sys),
# total wall, total user and sys CPU seconds.
set -u
B=/app/shopping-sessions
R=/scratch/runs
N=/scratch/notes
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/g >/dev/null 2>&1
run() {
  env "$@" PROBE_OUT=$R DATA_DIR=/scratch/g OUTPUT_DIR=/scratch/o $B 2>&1 | tail -1 >&2
  rm -rf /scratch/o
}
run NCPU=8
run NCPU=8 MMAPIO=1 PROBE_TAG=m
run NCPU=8
run NCPU=8 MMAPIO=1 PROBE_TAG=m
run NCPU=16
run NCPU=16 MMAPIO=1 PROBE_TAG=m
run NCPU=12 MMAPIO=1 PROBE_TAG=m
run NCPU=8 MMAPIO=1 NOHUGE=1 PROBE_TAG=mh
run NCPU=6 MMAPIO=1 PROBE_TAG=m
run NCPU=4 MMAPIO=1 PROBE_TAG=m
paste -d'|' - - < $R > $N
cat $N >&2
PROBE_NOTES=$N $B
