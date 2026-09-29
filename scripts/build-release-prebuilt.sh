#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: build-release-prebuilt.sh tag|nightly|snapshot}
commit=$(git rev-parse --short HEAD)
case "$mode" in
    tag)
        tag=$(git describe --tags --exact-match HEAD)
        version=${tag#v}
        ;;
    nightly)
        version="nightly-${commit}"
        ;;
    snapshot)
        version="dev-${commit}"
        ;;
    *)
        echo "unknown release mode: $mode" >&2
        exit 2
        ;;
esac

for arch in amd64 arm64; do
    output=".release-prebuilt/linux_${arch}"
    mkdir -p "$output"
    docker buildx build --platform "linux/$arch" --target release-artifacts \
        --build-arg "VERSION=$version" \
        --build-arg "COMMIT=$commit" \
        --build-arg "BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --output "type=local,dest=$output" .

    # Smoke the exported binaries in plain Alpine, without RocksDB or C++
    # runtime packages. A missing native dependency must fail before publish.
    for binary in ledger-server ledgerctl; do
        docker run --rm --platform "linux/$arch" \
            --mount "type=bind,src=$PWD/$output,dst=/artifacts,readonly" \
            --entrypoint "/artifacts/$binary" alpine:3.24 --help >/dev/null
    done
done
