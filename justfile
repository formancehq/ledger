set dotenv-load

# Go sub-modules relative to project root
# Go sub-modules that contain .go files (misc/devenv is Pulumi-only, no Go sources)
go_submodules := "misc/operator misc/benchmark-operator"

pre-commit: generate generate-proto tidy lint
pc: pre-commit

lint:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> golangci-lint (.)"
    golangci-lint run --fix --build-tags it,local,{{all_tags}} --timeout 5m
    for dir in {{go_submodules}}; do
        echo "==> golangci-lint ($dir)"
        (cd "$dir" && golangci-lint run --fix --timeout 5m)
    done

tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    for dir in . {{go_submodules}}; do
        echo "==> go mod tidy ($dir)"
        (cd "$dir" && go mod tidy)
    done

# All optional feature build tags
all_tags := "kafka,nats,clickhouse,s3,pyroscope"

# Build the server application (light: no optional deps)
build:
    go build -o ./build/ledger-server .

# Build the server with all optional features (Kafka, NATS, ClickHouse, S3, Pyroscope)
build-full:
    go build -tags "{{all_tags}}" -o ./build/ledger-server-full .

# Build the client application
build-client:
    go build -o ./build/ledgerctl ./cmd/ledgerctl

# Run the application locally (single node)
run:
    go run . run --node-id 1 --bind-addr 127.0.0.1:8888 --wal-dir ./wal/node-1 --data-dir ./data/node-1

# Run the client application
run-client *ARGS:
    go run ./cmd/ledgerctl {{ARGS}}

# Install ledgerctl and kubectl-ledger plugin into $(go env GOPATH)/bin
install: install-client install-kubectl-plugin

install-client:
    go build -o $(go env GOPATH)/bin/ledgerctl ./cmd/ledgerctl
    #todo: make optional or configurable or whatever
    ledgerctl completion zsh > ~/.oh-my-zsh/custom/completions/_ledgerctl

# Run unit tests for the root module (light build)
test:
    go test -race ./... -timeout 20m

# Run unit tests with all optional features
test-full:
    go test -race -tags "{{all_tags}}" ./... -timeout 20m

# Run all tests across all modules (unit + integration + e2e)
test-all: test test-submodules test-e2e test-scenarios

# Run unit tests for all sub-modules
test-submodules:
    #!/usr/bin/env bash
    set -euo pipefail
    for dir in {{go_submodules}}; do
        echo "==> go test ($dir)"
        (cd "$dir" && go test -race ./...)
    done

# Run end-to-end tests (light build)
test-e2e:
    go test -race -tags e2e -p 1 ./tests/e2e/business/... ./tests/e2e/cluster/... -timeout 20m

# Run end-to-end tests with all optional features
test-e2e-full:
    go test -race -tags "e2e,{{all_tags}}" -p 1 ./tests/e2e/business/... ./tests/e2e/cluster/... -timeout 20m

# Run financial scenario tests
test-scenarios:
    go test -race -tags scenario ./tests/scenarios/... -timeout 300s

# Release (official, triggered by tag)
release:
    goreleaser release --clean

# Release CI (nightly, triggered by main push)
release-ci:
    goreleaser release --nightly --clean

# Clean build artifacts
clean:
    rm -rf ledgerctl ledger-v3-poc
    rm $(find ./ -name '*.test') || true

clean-benchmarks-data:
    rm -rf build

generate:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> go generate (.)"
    rm $(find ./internal -name '*_generated_test.go') 2>/dev/null || true
    rm $(find ./internal -name '*_generated.go') 2>/dev/null || true
    go generate ./...
    echo "==> controller-gen (misc/operator)"
    cd misc/operator && go run sigs.k8s.io/controller-tools/cmd/controller-gen@latest \
        object:headerFile="" paths=./api/... \
        crd output:crd:dir=config/crd/bases \
        rbac:roleName=ledger-operator output:rbac:dir=config/rbac paths=./...

# Generate gRPC code from protobuf files
generate-proto:
    @echo "Generating gRPC code from proto files..."
    rm -f internal/proto/rafttransportpb/*.pb.go internal/proto/commonpb/*.pb.go internal/proto/servicepb/*.pb.go internal/proto/raftcmdpb/*.pb.go internal/proto/snapshotpb/*.pb.go internal/proto/clusterpb/*.pb.go internal/proto/auditpb/*.pb.go internal/proto/signaturepb/*.pb.go internal/proto/eventspb/*.pb.go internal/proto/restorepb/*.pb.go || true
    mkdir -p internal/proto/clusterpb internal/proto/rafttransportpb internal/proto/auditpb internal/proto/signaturepb internal/proto/eventspb internal/proto/restorepb
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
        misc/proto/bucket.proto \
        misc/proto/raft_cmd.proto \
        misc/proto/snapshot.proto \
        misc/proto/audit.proto \
        misc/proto/signature.proto \
        misc/proto/events.proto \
        misc/proto/restore.proto

# Docker builds are handled via Pulumi

k8s-install:
    cd misc/devenv && pulumi up

k8s-destroy:
    cd misc/devenv && pulumi destroy

k8s-watch:
    watch -n 1 kubectl get pods -l app.kubernetes.io/name=ledger-exp

k8s-logs:
    kubectl logs -f statefulset/ledger-exp

k8s-describe-ss:
    kubectl describe statefulset/ledger-exp

k8s-describe-pod:
    kubectl describe pods/ledger-exp-0

k8s-rollout-restart:
    kubectl rollout restart statefulsets/ledger-exp
    kubectl rollout status statefulset/ledger-exp

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

# Build the kubectl-ledger plugin
build-kubectl-plugin:
    cd misc/operator && go build -o ../../build/kubectl-ledger ./cmd/kubectl-ledger

# Install the kubectl-ledger plugin into $(go env GOPATH)/bin (makes it available as `kubectl ledger`)
install-kubectl-plugin:
    cd misc/operator && go build -o $(go env GOPATH)/bin/kubectl-ledger ./cmd/kubectl-ledger
