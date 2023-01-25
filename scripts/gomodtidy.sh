#!/usr/bin/env bash

dir=$(dirname "$0")
source "${dir}"/common.sh

for mod in $(find-updated-modules $@); do
  echo "Run go mod tidy on ${mod}"
  pushd ${mod} >/dev/null
  go mod tidy
  popd >/dev/null
done
