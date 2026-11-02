#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="$repo_root/build/hegel-smoke"
mkdir -p "$build_dir"

# Native Docker platform by default; CI runs this on Linux amd64. No registry
# push, instrumentor build, server build, or Composer discovery is involved.
env -u SOURCE_DATE_EPOCH docker build \
    --target hegel-smoke \
    --file "$repo_root/tests/antithesis/workload/Dockerfile" \
    --iidfile "$build_dir/image.id" \
    "$repo_root"
image_id="$(cat "$build_dir/image.id")"
docker image inspect "$image_id" --format '{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}'
if ! docker run --rm "$image_id" >"$build_dir/success.log" 2>&1; then
    cat "$build_dir/success.log" >&2
    exit 1
fi
cat "$build_dir/success.log"
grep -Fq 'hegel-smoke: PASS' "$build_dir/success.log"

# Remove the actual transitive native dependency in a disposable container.
# A Go-only fallback or a skipped callback must never turn this into success.
if docker run --rm --entrypoint /bin/sh "$image_id" -ec '
    test ! -e /opt/antithesis/test/v1
    library="$(find /usr/lib -name libgcc_s.so.1 -print -quit)"
    test -n "$library"
    rm "$library"
    exec /opt/antithesis/hegel-smoke
' >"$build_dir/missing-libgcc.log" 2>&1; then
    echo 'hegel-smoke: unexpectedly succeeded without libgcc_s.so.1' >&2
    exit 1
fi
cat "$build_dir/missing-libgcc.log"
grep -Fq 'libgcc_s.so.1' "$build_dir/missing-libgcc.log"
grep -Fq 'hegel-smoke:' "$build_dir/missing-libgcc.log"
if grep -Fq 'hegel-smoke: PASS' "$build_dir/missing-libgcc.log"; then
    echo 'hegel-smoke: missing dependency reported success' >&2
    exit 1
fi
echo 'hegel-smoke: PASS (missing libgcc_s.so.1 fails loudly)'
