#!/usr/bin/env bash

set -e

gomodules() {
  find components libs \( -name vendor -o -name '[._].*' -o -name node_modules \) -prune -o -name go.mod -print | sed 's:/go.mod$::'
}

find-updated-modules() {
  declare -A modules

  for file in $@; do
    for mod in $(gomodules); do
      [[ $file = $mod* ]] && modules[$mod]="yes"
    done
  done

  for mod in "${!modules[@]}"; do
    echo $mod
  done
}
