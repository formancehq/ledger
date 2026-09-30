#!/usr/bin/env bash
set -euo pipefail

# Compare the unreleased Pebble v3 baseline and the RocksDB PR on one runner.
# Invoke from the repository root, inside the Nix development shell.
pebble_ref=${PEBBLE_REF:?set PEBBLE_REF to the immutable Pebble commit}
rocksdb_ref=${ROCKSDB_REF:?set ROCKSDB_REF to the immutable RocksDB commit}
output_dir=${PERF_OUTPUT_DIR:-"$PWD/build/storage-performance"}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)

for ref in "$pebble_ref" "$rocksdb_ref"; do
  git cat-file -e "${ref}^{commit}"
done

work_dir=$(mktemp -d)
mkdir -p "$work_dir/bin"
cleanup() {
  git worktree remove --force "$work_dir/pebble" 2>/dev/null || true
  git worktree remove --force "$work_dir/rocksdb" 2>/dev/null || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

git worktree add --detach "$work_dir/pebble" "$pebble_ref"
git worktree add --detach "$work_dir/rocksdb" "$rocksdb_ref"

{
  printf 'Pebble SHA: %s\nRocksDB SHA: %s\n' "$pebble_ref" "$rocksdb_ref"
  printf 'Host: '; hostname
  printf 'Kernel: '; uname -a
  printf 'Go: '; go version
  lscpu || true
} > "$output_dir/environment.txt"

for engine in pebble rocksdb; do
  for package in dal readstore; do
    (cd "$work_dir/$engine" && go test -c -o "$work_dir/bin/$engine-$package.test" "./internal/storage/$package")
  done
done

export GOMAXPROCS=1
dal_pattern='^Benchmark(StoreGet|StoreGetValue|Batch_(Commit|5|100|1000))$'
index_pattern='^BenchmarkReverseMapKeying$/^account$/^field_first_production$/^PointLookupOneField$'

run_benchmarks() {
  local engine=$1
  local phase=$2
  local dal_output=$3
  local index_output=$4
  "$work_dir/bin/$engine-dal.test" -test.run='^$' -test.bench="$dal_pattern" -test.benchmem -test.benchtime=1s >> "$dal_output"
  "$work_dir/bin/$engine-readstore.test" -test.run='^$' -test.bench="$index_pattern" -test.benchmem -test.benchtime=1s >> "$index_output"
  printf '%s %s complete\n' "$engine" "$phase"
}

# Prime binaries, OS page cache, and database caches before recorded samples.
for engine in pebble rocksdb; do
  run_benchmarks "$engine" warmup "$output_dir/warmup-$engine-dal.txt" "$output_dir/warmup-$engine-index.txt"
done

for iteration in 1 2 3 4 5; do
  if (( iteration % 2 )); then
    order=(pebble rocksdb)
  else
    order=(rocksdb pebble)
  fi
  for engine in "${order[@]}"; do
    run_benchmarks "$engine" "sample $iteration" "$output_dir/$engine-dal.txt" "$output_dir/$engine-index.txt"
  done
done

python3 scripts/compare-storage-performance.py "$output_dir"
