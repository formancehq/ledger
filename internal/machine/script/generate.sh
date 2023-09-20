#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

cd "$(dirname "$0")"

ANTLR_VERSION='4.10.1'

main() {
  curl --continue-at - https://www.antlr.org/download/antlr-$ANTLR_VERSION-complete.jar -O
  java -Xmx500M -cp "./antlr-$ANTLR_VERSION-complete.jar" org.antlr.v4.Tool -Dlanguage=Go -o parser NumScript.g4
}

main "$@"
