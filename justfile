set dotenv-load

pre-commit: generate generate-proto operator-generate generate-dashboards tidy lint
pc: pre-commit

# Regenerate the Grafana dashboards (otel + prom variants) from Jsonnet
# sources under misc/devenv/monitoring-dashboards/jsonnet/. The output
# is written under misc/devenv/monitoring-dashboards/config/dashboards/
# and is consumed by Pulumi at deploy time.
generate-dashboards:
    #!/usr/bin/env bash
    set -euo pipefail
    cd misc/devenv/monitoring-dashboards
    if [ ! -d jsonnet/vendor ]; then
        echo "==> jb install (fetching grafonnet)"
        (cd jsonnet && jb install)
    fi
    echo "==> jsonnet -m config/dashboards jsonnet/main.jsonnet"
    rm -f config/dashboards/ledger-metrics*.json
    jsonnet -m config/dashboards -J jsonnet/vendor jsonnet/main.jsonnet

lint:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> golangci-lint (.)"
    golangci-lint run --fix --build-tags it,local,{{all_tags}} --timeout 5m
    echo "==> golangci-lint (operator)"
    cd misc/operator && golangci-lint run --fix --timeout 5m

tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> go mod tidy (.)"
    go mod tidy
    echo "==> go mod tidy (operator)"
    cd misc/operator && go mod tidy

# All optional feature build tags
all_tags := "kafka,nats,clickhouse,databricks,s3,azure,pyroscope"

# Docker image repository (registry + name). Override via env var to push
# to a different GHCR location. Default targets the canonical
# `formancehq/ledger` published image.
image_repository := env_var_or_default("IMAGE_REPOSITORY", "ghcr.io/formancehq/ledger")

# Operator Docker image repository (registry + name). Override via env var
# to push to a different GHCR location. Default targets the canonical
# `formancehq/ledger-operator` published image.
operator_image_repository := env_var_or_default("OPERATOR_IMAGE_REPOSITORY", "ghcr.io/formancehq/ledger-operator")

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
    go run . run --node-id 1 --cluster-id local-dev --bootstrap --bind-addr 127.0.0.1:7777 --grpc-port 8888 --wal-dir ./wal/node-1 --data-dir ./data/node-1

# Run the client application
run-client *ARGS:
    go run ./cmd/ledgerctl {{ARGS}}

# Install ledgerctl into $(go env GOPATH)/bin
install: install-client

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
test-all: test test-e2e test-scenarios

# Run end-to-end tests (light build)
test-e2e:
    go test -race -tags e2e -p 1 ./tests/e2e/business/... ./tests/e2e/cluster/... -timeout 20m

# Run end-to-end tests with all optional features
test-e2e-full:
    go test -race -tags "e2e,{{all_tags}}" -p 1 ./tests/e2e/business/... ./tests/e2e/cluster/... -timeout 20m

# Run financial scenario tests
test-scenarios:
    go test -race -tags scenario ./tests/scenarios/... -timeout 300s

# Run the in-memory model checker (singleton_driver_model) against a local
# single node for DURATION seconds. Validates FSM determinism / cache-Pebble
# consistency on every committed bulk. Exits non-zero on any finding.
test-model duration="60":
    tests/antithesis/run_model_test.sh {{duration}}

# Run the model checker against a 3-node cluster with rolling restarts —
# exercises leadership change, snapshot install and follower restore. This is
# the CI gate (the Tests-Model job runs it for 3 minutes); timing-sensitive.
test-model-cluster duration="180":
    tests/antithesis/run_model_test.sh --cluster {{duration}}

# Run all fuzz tests (seed corpus replay, no active fuzzing — fast CI check)
fuzz-check:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> Replaying fuzz seed corpora..."
    go test ./internal/proto/commonpb/ -run 'Fuzz' -timeout 60s
    go test ./internal/proto/servicepb/ -run 'Fuzz' -timeout 60s
    go test ./internal/proto/rafttransportpb/ -run 'Fuzz' -timeout 60s
    go test ./internal/proto/raftcmdpb/ -run 'Fuzz' -timeout 60s
    go test ./internal/proto/signaturepb/ -run 'Fuzz' -timeout 60s
    go test ./internal/proto/snapshotpb/ -run 'Fuzz' -timeout 60s
    go test ./internal/pkg/filterexpr/ -run 'Fuzz' -timeout 60s
    go test ./internal/pkg/semver/ -run 'Fuzz' -timeout 60s
    echo "All fuzz seed corpora passed."

# Run active fuzzing on all targets (default: 30s each)
fuzz duration="30s":
    #!/usr/bin/env bash
    set -euo pipefail
    targets=(
        "FuzzUint256UnmarshalJSON ./internal/proto/commonpb/"
        "FuzzMetadataSetUnmarshalJSON ./internal/proto/commonpb/"
        "FuzzTimestampUnmarshalJSON ./internal/proto/commonpb/"
        "FuzzLedgerLogUnmarshalJSON ./internal/proto/commonpb/"
        "FuzzConvertMetadataValue ./internal/proto/commonpb/"
        "FuzzBulkElementUnmarshalJSON ./internal/proto/servicepb/"
        "FuzzLedgerApplyRequestUnmarshalJSON ./internal/proto/servicepb/"
        "FuzzRaftRequestBatchUnmarshalVT ./internal/proto/rafttransportpb/"
        "FuzzSendMessageRequestUnmarshalVT ./internal/proto/rafttransportpb/"
        "FuzzProposalUnmarshalVT ./internal/proto/raftcmdpb/"
        "FuzzOrderUnmarshalVT ./internal/proto/raftcmdpb/"
        "FuzzStateUnmarshalVT ./internal/proto/raftcmdpb/"
        "FuzzSignedApplyBatchUnmarshalVT ./internal/proto/signaturepb/"
        "FuzzSignedLogUnmarshalVT ./internal/proto/signaturepb/"
        "FuzzFetchSnapshotResponseUnmarshalVT ./internal/proto/snapshotpb/"
        "FuzzFilterExprParse ./internal/pkg/filterexpr/"
        "FuzzSemverParse ./internal/pkg/semver/"
        "FuzzSemverParsePartial ./internal/pkg/semver/"
    )
    for entry in "${targets[@]}"; do
        name="${entry%% *}"
        pkg="${entry#* }"
        echo "==> Fuzzing $name ({{duration}})..."
        if ! go test "$pkg" -run '^$' -fuzz="^${name}$" -fuzztime="{{duration}}" -timeout 600s; then
            echo "FAIL: $name"
            exit 1
        fi
    done
    echo "All fuzz targets passed."

# Run active fuzzing on a single target
fuzz-one target duration="30s":
    go test ./... -run '^$' -fuzz="^{{target}}$" -fuzztime="{{duration}}" -timeout 600s

# Coverage output directory
coverage_dir := "build/coverage"

# Packages to instrument for coverage (exclude generated proto, test infra, misc)
coverage_pkgs := "github.com/formancehq/ledger/v3/internal/..."

# Run unit tests with coverage
test-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> Unit tests with coverage..."
    GOTOOLCHAIN=$(go env GOVERSION) go test -race -coverprofile={{coverage_dir}}/unit.out -coverpkg={{coverage_pkgs}} ./... -timeout 20m
    echo "Coverage profile: {{coverage_dir}}/unit.out"

# Run E2E tests with coverage
test-e2e-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> E2E tests with coverage..."
    GOTOOLCHAIN=$(go env GOVERSION) go test -race -tags e2e -p 1 -coverprofile={{coverage_dir}}/e2e.out -coverpkg={{coverage_pkgs}} ./tests/e2e/business/... ./tests/e2e/cluster/... -timeout 20m
    echo "Coverage profile: {{coverage_dir}}/e2e.out"

# Run scenario tests with coverage
test-scenarios-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> Scenario tests with coverage..."
    GOTOOLCHAIN=$(go env GOVERSION) go test -race -tags scenario -coverprofile={{coverage_dir}}/scenario.out -coverpkg={{coverage_pkgs}} ./tests/scenarios/... -timeout 300s
    echo "Coverage profile: {{coverage_dir}}/scenario.out"

# Run fuzz seed replay with coverage
fuzz-check-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    fuzz_pkgs=(
        ./internal/proto/commonpb/
        ./internal/proto/servicepb/
        ./internal/proto/rafttransportpb/
        ./internal/proto/raftcmdpb/
        ./internal/proto/signaturepb/
        ./internal/proto/snapshotpb/
        ./internal/pkg/filterexpr/
        ./internal/pkg/semver/
    )
    echo "==> Fuzz seed replay with coverage..."
    GOTOOLCHAIN=$(go env GOVERSION) go test -coverprofile={{coverage_dir}}/fuzz.out -coverpkg={{coverage_pkgs}} -run 'Fuzz' -timeout 60s "${fuzz_pkgs[@]}"
    echo "Coverage profile: {{coverage_dir}}/fuzz.out"

# Merge all coverage profiles into a single report
coverage-merge:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    profiles=()
    for f in {{coverage_dir}}/unit.out {{coverage_dir}}/e2e.out {{coverage_dir}}/scenario.out {{coverage_dir}}/fuzz.out; do
        [ -f "$f" ] && profiles+=("$f")
    done
    if [ ${#profiles[@]} -eq 0 ]; then
        echo "No coverage profiles found in {{coverage_dir}}/"
        exit 1
    fi
    echo "==> Merging ${#profiles[@]} coverage profiles..."
    # Take the mode line from the first profile, then all data lines from all profiles
    head -1 "${profiles[0]}" > {{coverage_dir}}/merged.out
    for f in "${profiles[@]}"; do
        tail -n +2 "$f" >> {{coverage_dir}}/merged.out
    done
    echo "==> Merged profile: {{coverage_dir}}/merged.out"
    go tool cover -func={{coverage_dir}}/merged.out | tail -1
    echo ""
    echo "To view HTML report: go tool cover -html={{coverage_dir}}/merged.out -o {{coverage_dir}}/coverage.html"

# Generate HTML coverage report from merged profile
coverage-html: coverage-merge
    go tool cover -html={{coverage_dir}}/merged.out -o {{coverage_dir}}/coverage.html
    echo "HTML report: {{coverage_dir}}/coverage.html"

# Run all tests with coverage and merge
coverage-all: test-coverage test-e2e-coverage test-scenarios-coverage fuzz-check-coverage coverage-merge

# Run Schemathesis API conformity and fuzzing tests
test-schemathesis:
    bash tests/schemathesis/run.sh

# Release (official, triggered by tag)
release:
    goreleaser release --clean

# Release CI (nightly, triggered by main push)
release-ci:
    goreleaser release --nightly --clean

# Clean build artifacts
clean:
    rm -rf ledgerctl ledger
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

# Generate gRPC code from protobuf files
generate-proto:
    @echo "Generating gRPC code from proto files..."
    rm -f internal/proto/rafttransportpb/*.pb.go internal/proto/commonpb/*.pb.go internal/proto/servicepb/*.pb.go internal/proto/raftcmdpb/*.pb.go internal/proto/snapshotpb/*.pb.go internal/proto/clusterpb/*.pb.go internal/proto/clusterbootstrappb/*.pb.go internal/proto/auditpb/*.pb.go internal/proto/signaturepb/*.pb.go internal/proto/eventspb/*.pb.go internal/proto/restorepb/*.pb.go internal/proto/proposalpb/*.pb.go || true
    mkdir -p internal/proto/clusterpb internal/proto/clusterbootstrappb internal/proto/rafttransportpb internal/proto/auditpb internal/proto/signaturepb internal/proto/eventspb internal/proto/restorepb internal/proto/proposalpb
    @cd tools/protoc-gen-dethash && go build -o ../../build/protoc-gen-dethash .
    @cd tools/protoc-gen-reader && go build -o ../../build/protoc-gen-reader .
    @cd tools/protoc-gen-skippable && go build -o ../../build/protoc-gen-skippable .
    @protoc --go_out=. --go_opt=module=github.com/formancehq/ledger/v3 \
        --go-grpc_out=. \
        --go-grpc_opt=module=github.com/formancehq/ledger/v3 \
        --go-vtproto_out=. \
        --go-vtproto_opt=module=github.com/formancehq/ledger/v3 \
        --go-vtproto_opt=features=marshal+unmarshal+size+clone+equal+pool \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.Proposal \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.ExecutionPlan \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.AttributeValue \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.AttributePlan \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.Order \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.TechnicalUpdate \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.EventsSinkUpdate \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/raftcmdpb.MirrorSyncUpdate \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/auditpb.AuditEntry \
        --go-vtproto_opt=pool=github.com/formancehq/ledger/v3/internal/proto/proposalpb.AppliedProposal \
        --plugin=protoc-gen-dethash=build/protoc-gen-dethash \
        --dethash_out=. \
        --dethash_opt=module=github.com/formancehq/ledger/v3 \
        --plugin=protoc-gen-reader=build/protoc-gen-reader \
        --reader_out=. \
        --reader_opt=module=github.com/formancehq/ledger/v3 \
        --plugin=protoc-gen-skippable=build/protoc-gen-skippable \
        --skippable_out=. \
        --skippable_opt=module=github.com/formancehq/ledger/v3 \
        -I misc/proto \
        misc/proto/raft_transport.proto \
        misc/proto/common.proto \
        misc/proto/cluster.proto \
        misc/proto/cluster_bootstrap.proto \
        misc/proto/bucket.proto \
        misc/proto/raft_cmd.proto \
        misc/proto/snapshot.proto \
        misc/proto/audit.proto \
        misc/proto/signature.proto \
        misc/proto/events.proto \
        misc/proto/restore.proto \
        misc/proto/proposal.proto

# --- Operator (Kubernetes) ---

# controller-gen binary for CRD/RBAC generation
controller-gen := "go run sigs.k8s.io/controller-tools/cmd/controller-gen@latest"

# Generate operator CRDs, RBAC, and sync Helm chart
operator-generate:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> controller-gen (operator)"
    cd misc/operator
    {{controller-gen}} object:headerFile="" paths=./api/... \
        crd output:crd:dir=config/crd/bases \
        rbac:roleName=ledger-operator output:rbac:dir=config/rbac paths=./...
    ./scripts/sync-chart-rbac.sh
    cp config/crd/bases/*.yaml helm/crds/templates/

# Build the operator binary
operator-build:
    cd misc/operator && go build -o build/operator ./cmd/operator

# Build the kubectl-ledger plugin
operator-build-plugin:
    cd misc/operator && go build -o build/kubectl-ledger ./cmd/kubectl-ledger

# Install the kubectl-ledger plugin into $GOPATH/bin
operator-install-plugin:
    cd misc/operator && go build -o $(go env GOPATH)/bin/kubectl-ledger ./cmd/kubectl-ledger

# Run operator unit tests
operator-test:
    cd misc/operator && go test ./...

# Run operator pre-commit checks (generate + tidy + build)
operator-pre-commit: operator-generate
    #!/usr/bin/env bash
    set -euo pipefail
    cd misc/operator
    echo "==> operator: go mod tidy"
    go mod tidy
    echo "==> operator: go build"
    go build ./...

# Build and push operator Docker image (multi-arch). With a tag argument, also tags :latest.
operator-docker-build tag='':
    #!/bin/bash
    set -euo pipefail
    image='{{ operator_image_repository }}'
    if [ -n "{{ tag }}" ]; then
        docker buildx build -t "${image}:{{ tag }}" -t "${image}:latest" \
            --platform linux/amd64,linux/arm64 --push misc/operator
    else
        docker buildx build -t "${image}:latest" \
            --platform linux/amd64,linux/arm64 --push misc/operator
    fi

# Package and publish operator Helm charts to GHCR. Args are positional: version then suffix.
operator-helm-publish version='' suffix='':
    cd misc/operator && just helm-publish '{{ version }}' '{{ suffix }}'

# Build and push multi-arch Docker image
docker-build *ARGS:
    docker buildx build -t '{{ image_repository }}' --platform linux/amd64,linux/arm64 --push --build-arg BUILD_TAGS=kafka,clickhouse,s3,azure,pyroscope {{ARGS}} .

# Build and push a PR docker image under a single explicit tag (no `:latest`).
# Used by the `Build-PR-Image` workflow job to publish
# `{{ image_repository }}:pr-<num>-<sha7>` for smoke tests
# before merge — keeps the same BUILD_TAGS set as `docker-build` so a
# reviewer exercises the full optional-feature surface.
docker-build-pr image:
    docker buildx build -t '{{ image }}' --platform linux/amd64,linux/arm64 --push --build-arg BUILD_TAGS=kafka,clickhouse,s3,azure,pyroscope .

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
        PATH="{{justfile_directory()}}/build:$PATH" LEDGERCTL_SERVER="127.0.0.1:8888" LEDGERCTL_INSECURE="true" vhs "misc/demo/${tape}.tape"
        echo "==> Done: misc/demo/${tape}.gif"
    done
    echo "All demos generated."
