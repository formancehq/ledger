#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -f build/client-release-provenance.json

server_tag=${1:?server version tag is required}
compatibility=misc/release/public-client.json
client_version=$(jq -er '.clientVersion' "$compatibility")
client_tag="pkg/client/${client_version}"
[[ "$(jq -er '.serverVersion' "$compatibility")" == "$server_tag" ]] || {
    echo "server compatibility metadata does not name $server_tag" >&2
    exit 1
}

release_commit=$(git rev-parse 'HEAD^{commit}')
client_commit=$(git rev-parse "refs/tags/${client_tag}^{commit}")
if ! git diff --quiet "$client_commit" "$release_commit" -- pkg/client/v3; then
    echo "client source at $release_commit differs from immutable tag $client_tag" >&2
    exit 1
fi
required_version=$(GOWORK=off go list -m -f '{{.Version}}' github.com/formancehq/ledger/pkg/client/v3)
[[ "$required_version" == "$client_version" ]] || {
    echo "root module requires client $required_version, expected $client_version" >&2
    exit 1
}

python3 scripts/check-client-generation.py
download_dir=$(mktemp -d)
trap 'rm -r "$download_dir"' EXIT
printf 'module example.com/ledger-client-release-check\n\ngo 1.26.0\n' > "$download_dir/go.mod"
download=$(GOPROXY=direct GOSUMDB=off GOWORK=off go -C "$download_dir" mod download -json "github.com/formancehq/ledger/pkg/client/v3@${client_version}")
module_sum=$(jq -er '.Sum' <<<"$download")
module_dir=$(jq -er '.Dir' <<<"$download")
mkdir -p build
provenance=$(mktemp build/client-release-provenance.XXXXXX)
trap 'rm -r "$download_dir"; rm -f "$provenance"' EXIT
python3 scripts/verify_client_release_contract.py "$PWD" "$server_tag" "$module_dir" \
    "$client_tag" "$client_commit" "$release_commit" "$module_sum" > "$provenance"
mv "$provenance" build/client-release-provenance.json
