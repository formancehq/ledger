#!/usr/bin/env bash

dir=$(dirname "$0")
source "${dir}"/common.sh

for mod in $(find-updated-modules $@); do
  echo "Run golangci-lint on ${mod}"
  pushd ${mod} >/dev/null
  golangci-lint -v run --fix
  popd >/dev/null
done
