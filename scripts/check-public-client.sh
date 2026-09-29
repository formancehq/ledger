#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 scripts/check-client-generation.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_verify_client_release_contract.py
server_version=$(jq -er '.serverVersion' misc/release/public-client.json)
client_version=$(jq -er '.clientVersion' misc/release/public-client.json)
required_version=$(GOWORK=off go list -m -f '{{.Version}}' github.com/formancehq/ledger/pkg/client/v3)
[[ "$required_version" == "$client_version" ]] || {
    echo "root module requires client $required_version, expected $client_version" >&2
    exit 1
}
python3 scripts/verify_client_release_contract.py "$PWD" "$server_version" \
    "$PWD/pkg/client/v3" "pkg/client/${client_version}" local local h1:local \
    --local-contract >/dev/null
GOWORK=off go -C pkg/client/v3 test ./...

if GOWORK=off go -C pkg/client/v3 list -deps -f '{{.ImportPath}}' ./... |
    grep -E '^github.com/formancehq/ledger/v3/|^github.com/cockroachdb/pebble/' ; then
    echo 'public client imports server-only dependencies' >&2
    exit 1
fi

external_dir=$(mktemp -d)
trap 'rm -r "$external_dir"' EXIT
cat > "$external_dir/go.mod" <<'EOF'
module example.com/ledger-client-contract-check

go 1.26.0

require github.com/formancehq/ledger/pkg/client/v3 v3.0.0-beta.6
EOF
cat > "$external_dir/client_test.go" <<'EOF'
package clientcheck

import (
    "context"
    "testing"

    ledgergrpc "github.com/formancehq/ledger/pkg/client/v3/grpc"
    "google.golang.org/grpc"
)

func TestExternalClientBuilds(t *testing.T) {
    var conn *grpc.ClientConn
    _ = ledgergrpc.ClientOption()
    client := ledgergrpc.NewBucketServiceClient(conn)
    _ = func(ctx context.Context) error {
        _, err := client.Discovery(ctx, &ledgergrpc.DiscoveryRequest{})
        return err
    }
}
EOF
GOWORK=off go -C "$external_dir" mod edit \
    -replace="github.com/formancehq/ledger/pkg/client/v3=$(pwd)/pkg/client/v3"
GOWORK=off go -C "$external_dir" mod tidy
GOWORK=off go -C "$external_dir" test ./...

go test ./pkg/testserver -run TestPublicClientSharesServerDescriptorRegistry -count=1
