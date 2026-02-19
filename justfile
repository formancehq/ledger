set dotenv-load

pre-commit: generate generate-proto tidy lint
pc: pre-commit

lint:
    golangci-lint run --fix --build-tags it,local --timeout 5m

tidy:
    go mod tidy

# Build the server application
build:
    go build -o ./build/ledger-server .

# Build the client application
build-client:
    go build -o ./build/ledgerctl ./cmd/ledgerctl

# Run the application locally (single node)
run:
    go run . run --node-id 1 --bind-addr 127.0.0.1:8888 --wal-dir ./wal/node-1 --data-dir ./data/node-1

# Run the client application
run-client *ARGS:
    go run ./cmd/ledgerctl {{ARGS}}

install-client:
    go build -o $GOPATH/bin/ledgerctl ./cmd/ledgerctl
    #todo: make optional or configurable or whatever
    ledgerctl completion zsh > ~/.oh-my-zsh/custom/completions/_ledgerctl

# Run tests
test:
    go test ./... -tags it,e2e

# Clean build artifacts
clean:
    rm -rf ledgerctl ledger-v3-poc
    rm $(find ./ -name '*.test') || true

clean-benchmarks-data:
    rm -rf build

generate:
    rm $(find ./internal -name '*_generated_test.go') || true
    rm $(find ./internal -name '*_generated.go') || true
    go generate ./...

# Generate gRPC code from protobuf files
generate-proto:
    @echo "Generating gRPC code from proto files..."
    rm -f internal/proto/rafttransportpb/*.pb.go internal/proto/commonpb/*.pb.go internal/proto/servicepb/*.pb.go internal/proto/raftcmdpb/*.pb.go internal/proto/snapshotpb/*.pb.go internal/proto/clusterpb/*.pb.go internal/proto/auditpb/*.pb.go internal/proto/signaturepb/*.pb.go internal/proto/eventspb/*.pb.go || true
    mkdir -p internal/proto/clusterpb internal/proto/rafttransportpb internal/proto/auditpb internal/proto/signaturepb internal/proto/eventspb
    @protoc --go_out=. --go_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-grpc_out=. \
        --go-grpc_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-vtproto_out=. \
        --go-vtproto_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-vtproto_opt=features=marshal+unmarshal+size+clone+equal \
        -I misc/proto \
        misc/proto/raft_transport.proto \
        misc/proto/common.proto \
        misc/proto/cluster.proto \
        misc/proto/service.proto \
        misc/proto/raftcmd.proto \
        misc/proto/snapshot.proto \
        misc/proto/audit.proto \
        misc/proto/signature.proto \
        misc/proto/events.proto

# Docker builds are handled via Pulumi

k8s-install:
    cd misc/devenv && pulumi up

k8s-uninstall:
    helm uninstall ledger-v3-poc

k8s-watch:
    watch -n 1 kubectl get pods -l app.kubernetes.io/name=ledger-v3-poc

k8s-logs:
    kubectl logs -f statefulset/ledger-v3-poc

k8s-describe-ss:
    kubectl describe statefulset/ledger-v3-poc

k8s-describe-pod:
    kubectl describe pods/ledger-v3-poc-0

k8s-rollout-restart:
    kubectl rollout restart statefulsets/ledger-v3-poc
    kubectl rollout status statefulset/ledger-v3-poc

# Available demo tapes
demos := "demo_getting_started demo_numscript demo_transactions demo_metadata demo_operations demo_audit demo_signing"

# Generate all CLI demo GIFs (starts a temporary server automatically)
generate-demo: (_generate-demo demos)

# Generate a single CLI demo GIF
# Usage: just generate-demo-only demo_numscript
generate-demo-only name: (_generate-demo name)

_generate-demo tapes:
    #!/usr/bin/env bash
    set -euo pipefail
    DEMO_DIR=$(mktemp -d)
    echo "Generating CLI demo GIFs..."
    echo "Using temporary directory: $DEMO_DIR"
    echo "Building client..."
    go build -o ./build/ledgerctl ./cmd/ledgerctl
    echo "Building server..."
    go build -o "$DEMO_DIR/ledger-server" .
    cleanup() {
        echo "Stopping server (PID: $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        rm -rf "$DEMO_DIR"
        echo "Cleanup done."
    }
    trap cleanup EXIT
    "$DEMO_DIR/ledger-server" run \
        --node-id 1 \
        --bootstrap \
        --bind-addr 127.0.0.1:7777 \
        --wal-dir "$DEMO_DIR/wal" \
        --data-dir "$DEMO_DIR/data" \
        --grpc-port 8888 \
        &
    SERVER_PID=$!
    echo "Waiting for server to be ready (PID: $SERVER_PID)..."
    for i in $(seq 1 30); do
        if grpcurl -plaintext 127.0.0.1:8888 grpc.health.v1.Health/Check > /dev/null 2>&1; then
            echo "Server is ready."
            break
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "Server process died unexpectedly."
            exit 1
        fi
        sleep 1
    done
    if ! grpcurl -plaintext 127.0.0.1:8888 grpc.health.v1.Health/Check > /dev/null 2>&1; then
        echo "Server failed to start within 30 seconds."
        exit 1
    fi
    for tape in {{tapes}}; do
        echo "==> Generating $tape..."
        PATH="{{justfile_directory()}}/build:$PATH" SERVER="127.0.0.1:8888" INSECURE="true" vhs "misc/demo/${tape}.tape"
        echo "==> Done: misc/demo/${tape}.gif"
    done
    echo "All demos generated."
