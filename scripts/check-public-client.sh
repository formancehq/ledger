#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 scripts/check-client-generation.py
python3 scripts/check-public-client-surface.py
[[ "$(go -C pkg/client/v3 list -m -f '{{.Path}}')" == "github.com/formancehq/ledger/pkg/client/v3" ]] || {
    echo "public client module path is invalid" >&2
    exit 1
}
GOWORK=off go -C pkg/client/v3 test ./...

if GOWORK=off go -C pkg/client/v3 list -deps -f '{{.ImportPath}}' ./... |
    grep -E '^(github.com/formancehq/ledger/v3/|github.com/cockroachdb/pebble/|github.com/formancehq/go-libs/v5/|github.com/holiman/uint256$|github.com/invopop/jsonschema$|database/sql(/|$))' ; then
    echo 'public client imports server-only dependencies' >&2
    exit 1
fi

external_dir=$(mktemp -d)
trap 'rm -r "$external_dir"' EXIT
cat > "$external_dir/go.mod" <<'EOF'
module example.com/ledger-client-contract-check

go 1.26.0

require github.com/formancehq/ledger/pkg/client/v3 v3.0.0-beta.7
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
