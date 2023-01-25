#!/usr/bin/env bash

dir=$(dirname "$0")
source "${dir}"/common.sh

for mod in $(find-updated-modules $@); do
  echo "Run tests on ${mod}"
  pushd ${mod} >/dev/null
  [[ -e Taskfile.yml ]] && task tests
  popd >/dev/null
done

echo "Run $cmdLine"
$cmdLine
