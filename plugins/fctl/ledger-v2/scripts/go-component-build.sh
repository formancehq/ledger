#!/usr/bin/env bash
set -euo pipefail

arguments=()
output_path=""
previous=""
for argument in "$@"; do
  if [[ "$argument" == '-ldflags=-checklinkname=0' ]]; then
    argument='-ldflags=-checklinkname=0 -buildid= -s -w'
  fi
  if [[ "$previous" == '-o' ]]; then
    output_path="$argument"
  fi
  arguments+=("$argument")
  previous="$argument"
done
go "${arguments[@]}"
if [[ -n "$output_path" ]]; then
  wasm-opt -Oz --all-features "$output_path" --output "$output_path.optimized"
  mv "$output_path.optimized" "$output_path"
fi
