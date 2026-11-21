#!/bin/sh
# Records pgo/merged.profdata, the profile the Docker build optimises with.
# Rerun after any change to src/ or Cargo.lock: the profile must come from
# the same source and the same toolchain as the Dockerfile's (rust 1.97.0).
# usage: scripts/pgo.sh <generator binary> <dataset dir>...
#   each dataset dir is used as is if it exists, otherwise generated there
#   (seed 99, 3M events) with the generator
set -eu
GEN=$1
shift
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROFDATA=${PROFDATA:-llvm-profdata}
FLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"
RAW=$(mktemp -d)
cd "$ROOT"
RUSTFLAGS="$FLAGS -C profile-generate=$RAW" cargo build --release --locked --target-dir target/pgo-gen
for d in "$@"; do
  [ -d "$d" ] || "$GEN" --seed 99 --events 3000000 --clicks 750000 --output "$d" >/dev/null
  out=$(mktemp -d)
  DATA_DIR="$d" OUTPUT_DIR="$out/o" NCPU=2 target/pgo-gen/release/shopping-sessions 2>/dev/null
  rm -rf "$out"
done
mkdir -p pgo
"$PROFDATA" merge -o pgo/merged.profdata "$RAW"
rm -rf "$RAW"
echo "wrote pgo/merged.profdata"
