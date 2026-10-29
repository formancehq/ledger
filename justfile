set dotenv-load

pre-commit: fuzz-inventory-check generate generate-proto test-rpcauth-generator test-buf-plugin lint-proto operator-generate test-dashboards tidy lint
pc: pre-commit

# Compare the service contract with the current PR or merge-queue target.
check-protocol-version:
    go run ./scripts/checkprotocolversion --base "$PROTOCOL_BASE_SHA"

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

# Regenerate and validate the committed Grafana dashboard variants.
test-dashboards: generate-dashboards
    cd misc/devenv/monitoring-dashboards && go test ./...

lint:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> golangci-lint (.)"
    golangci-lint run --fix --build-tags it,local,enable_antithesis_sdk,{{all_tags}} --timeout 5m
    echo "==> golangci-lint (operator)"
    (cd misc/operator && golangci-lint run --fix --timeout 5m)
    echo "==> golangci-lint (model workload)"
    (cd tests/antithesis/workload && golangci-lint run --fix --build-tags enable_antithesis_sdk --timeout 5m)

# Go modules kept tidy by `just tidy` (and checked by the Dirty CI job).
tidy_modules := ". misc/operator tests/antithesis/workload"

tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    for dir in {{tidy_modules}}; do
        echo "==> go mod tidy ($dir)"
        go -C "$dir" mod tidy
    done

# Download every Go module CI needs, plus the pinned controller-gen, into
# GOMODCACHE without modifying any file. CI runs this in Setup Env so every job
# commits the full module set to the shared Namespace cache volume: the last
# job to commit on a node decides what the next job forks, so modules only one
# job needs would otherwise be lost. tests/perf is excluded because no CI job
# builds it.
go-mod-download:
    #!/usr/bin/env bash
    set -euo pipefail
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    i=0
    for dir in {{tidy_modules}} misc/devenv/monitoring-dashboards tools/*/; do
        dir=${dir%/}
        echo "==> go mod download ($dir)"
        # Without arguments, `go mod download` may rewrite inconsistent
        # requirements in go.mod and add checksums to go.sum. Point it at
        # copies so the checkout stays exactly as committed.
        i=$((i + 1))
        cp "$dir/go.mod" "$work/$i.mod"
        if [ -f "$dir/go.sum" ]; then
            cp "$dir/go.sum" "$work/$i.sum"
        fi
        go -C "$dir" mod download -modfile="$work/$i.mod"
    done
    # `go mod tidy` also loads the tests of dependencies, whose modules
    # `go mod download` skips. `go mod tidy -diff` resolves the same set without
    # writing go.mod/go.sum. It exits 1 and prints the diff on stdout when a
    # module is untidy, which the Dirty job reports, so only that case is not
    # fatal here.
    for dir in {{tidy_modules}}; do
        echo "==> go mod tidy -diff ($dir)"
        status=0
        go -C "$dir" mod tidy -diff >"$work/tidy.diff" || status=$?
        if [ "$status" -eq 1 ] && [ -s "$work/tidy.diff" ]; then
            echo "$dir is not tidy; run 'just tidy'"
        elif [ "$status" -ne 0 ]; then
            exit "$status"
        fi
    done
    echo "==> controller-gen (download and build)"
    {{controller-gen}} --version

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

# Compile the packages that hold guarded code with the Antithesis SDK armed and
# run the assertion-emission contract tests. The trees below are the target's
# reach, and check-repo-invariants fails any `if assert.Enabled` outside them,
# so adding a guard elsewhere means widening this list.
#
# The SDK is pinned to the fork's default-no-op build, which compiles to no-ops
# unless enable_antithesis_sdk is set, so the default suite above cannot
# observe a single assertion emit.
#
# It carries a coverage profile because the guarded assertion branches are
# reachable only in this shape: without it those lines are missing from every
# uploaded profile and the patch-coverage gate fails on code this suite does
# execute. coverage-merge folds antithesis.out in with the rest.
#
# It runs every test in the packages that hold guarded code, not just
# TestAntithesis*: the emission tests do their work in a re-exec'd subprocess,
# whose coverage never reaches this profile, so the guarded lines are only
# counted when ordinary tests execute them in-process against an armed build.
test-antithesis-assertions:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    scripts/go-test-coverage {{coverage_dir}}/antithesis.out \
        -race -tags enable_antithesis_sdk -coverpkg={{coverage_pkgs}} -timeout 20m \
        ./internal/infra/state/... ./internal/infra/node/... \
        ./internal/domain/processing/... ./internal/query/... \
        ./internal/application/ctrl/...

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

# Verify the active fuzz runner inventory matches Go fuzz declarations.
fuzz-inventory-check:
    bash scripts/check-repo-invariants --fuzz-inventory

# Run active fuzzing on all targets (default: 30s each)
fuzz duration="30s": fuzz-inventory-check
    #!/usr/bin/env bash
    set -euo pipefail
    while read -r pkg name || [[ -n "${pkg:-}${name:-}" ]]; do
        echo "==> Fuzzing $name ({{duration}})..."
        if ! go test "$pkg" -run '^$' -fuzz="^${name}$" -fuzztime="{{duration}}" -timeout 600s; then
            echo "FAIL: $name"
            exit 1
        fi
    done < scripts/fuzz-targets.txt
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
    scripts/go-test-coverage {{coverage_dir}}/unit.out -race -coverpkg={{coverage_pkgs}} -timeout 20m ./...

# Execute internal and offline bootstrap assertions behind optional feature tags (requires Docker).
test-internal-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> Internal tests with optional features and coverage..."
    scripts/go-test-coverage {{coverage_dir}}/internal.out -race -tags "{{all_tags}}" -coverpkg={{coverage_pkgs}} -timeout 20m ./internal/... ./cmd/ledgerctl/store

# Run PostgreSQL mirror integration regressions (requires Docker or LEDGER_TEST_POSTGRES_DSN).
test-postgres-mirror-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    scripts/go-test-coverage {{coverage_dir}}/postgres-mirror.out \
        -race -tags integration -coverpkg={{coverage_pkgs}} \
        -run Postgres -count=1 -timeout 5m \
        ./internal/adapter/v2 ./internal/application/mirror

# Run E2E tests with coverage. The CI gates invoke this recipe once for each
# isolated Business/Cluster job. Every invocation uses the full tag set: the
# feature-gated suites (s3, azure, clickhouse, nats, ...) are only compiled
# when their tag is present, and running them here is what stops them rotting
# unnoticed. Suites needing external credentials, such as databricks, skip
# themselves when the environment is not configured. With no arguments the
# recipe retains the combined, serial local coverage run.
test-e2e-coverage packages="./tests/e2e/business/... ./tests/e2e/cluster/..." profile="e2e.out":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> E2E tests with coverage..."
    scripts/go-test-coverage {{coverage_dir}}/{{profile}} -race -tags "e2e,{{all_tags}}" -p 1 -coverpkg={{coverage_pkgs}} -timeout 30m {{packages}}

# Run scenario tests with coverage
test-scenarios-coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    echo "==> Scenario tests with coverage..."
    scripts/go-test-coverage {{coverage_dir}}/scenario.out -race -tags scenario -coverpkg={{coverage_pkgs}} -timeout 300s ./tests/scenarios/...

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
    scripts/go-test-coverage {{coverage_dir}}/fuzz.out -coverpkg={{coverage_pkgs}} -run 'Fuzz' -timeout 60s "${fuzz_pkgs[@]}"

# Merge all coverage profiles into a single report
coverage-merge:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{coverage_dir}}
    profiles=()
    for f in {{coverage_dir}}/unit.out {{coverage_dir}}/internal.out {{coverage_dir}}/antithesis.out {{coverage_dir}}/e2e.out {{coverage_dir}}/scenario.out {{coverage_dir}}/fuzz.out; do
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
coverage-all: test-coverage test-internal-coverage test-antithesis-assertions test-e2e-coverage test-scenarios-coverage fuzz-check-coverage coverage-merge

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
    @cd tools/protoc-gen-queryfilter-validity && go build -o ../../build/protoc-gen-queryfilter-validity .
    @cd tools/protoc-gen-ledger-log-category && go build -o ../../build/protoc-gen-ledger-log-category .
    @cd tools/protoc-gen-rpcauth && go build -o ../../build/protoc-gen-rpcauth .
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
        --plugin=protoc-gen-queryfilter-validity=build/protoc-gen-queryfilter-validity \
        --queryfilter-validity_out=. \
        --queryfilter-validity_opt=module=github.com/formancehq/ledger/v3 \
        --plugin=protoc-gen-ledger-log-category=build/protoc-gen-ledger-log-category \
        --ledger-log-category_out=. \
        --ledger-log-category_opt=module=github.com/formancehq/ledger/v3 \
        --plugin=protoc-gen-rpcauth=build/protoc-gen-rpcauth \
        --rpcauth_out=. \
        --rpcauth_opt=module=github.com/formancehq/ledger/v3 \
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

test-rpcauth-generator:
    go -C tools/protoc-gen-rpcauth test ./...

# Initial-layout conventions for new protobuf types via the buf check plugin
# in tools/buf-plugin-ledger; existing types retain their published numbers.
lint-proto:
    go run ./scripts/lintproto

test-buf-plugin:
    go -C tools/buf-plugin-ledger test -race ./...

# --- Operator (Kubernetes) ---

# controller-gen binary for CRD/RBAC generation
controller-gen := "go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0"

# Generate operator CRDs, RBAC, and sync Helm chart
operator-generate:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> controller-gen (operator)"
    cd misc/operator
    {{controller-gen}} object:headerFile="" paths=./api/... \
        crd output:crd:dir=config/crd/bases \
        rbac:roleName=ledger-operator output:rbac:dir=config/rbac paths=./...
    go run ./scripts/generate-json-schemas
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

# Run Ledger target admission regressions in the operator-tests Nix shell.
operator-test-admission:
    #!/usr/bin/env bash
    set -euo pipefail
    unset GOROOT
    KUBEBUILDER_ASSETS=$(setup-envtest use 1.32.x -p path)
    export KUBEBUILDER_ASSETS
    cd misc/operator
    go test -race -tags integration ./internal/controller -run '^TestLedgerAdmission' -count=1 -timeout 3m

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

# Build and push a PR operator image with GoReleaser under one explicit
# `<repository>:pr-<num>-<sha7>` tag. It never touches :latest, a GitHub release
# or the Helm charts. Needs GORELEASER_KEY and a Docker login to the registry.
# Config: misc/operator/.goreleaser.pr.yml.
operator-docker-build-pr image: (_assert-pr-image image)
    #!/usr/bin/env bash
    set -euo pipefail
    image='{{ image }}'
    export OPERATOR_IMAGE_REPOSITORY="${image%:*}"
    export PR_IMAGE_TAG="${image##*:}"
    cd misc/operator
    # GITHUB_TOKEN is removed on purpose: a PR build must never reach the GitHub
    # releases API, even if a release step were enabled by mistake.
    env -u GITHUB_TOKEN -u GH_TOKEN \
        goreleaser release --nightly --clean --config .goreleaser.pr.yml --skip=archive,release

# Package and publish operator Helm charts to GHCR. Args are positional: version then suffix.
operator-helm-publish version='' suffix='':
    cd misc/operator && just helm-publish '{{ version }}' '{{ suffix }}'

# Build and push multi-arch Docker image
docker-build *ARGS:
    docker buildx build -t '{{ image_repository }}' --platform linux/amd64,linux/arm64 --push --build-arg BUILD_TAGS=kafka,nats,clickhouse,s3,azure,pyroscope {{ARGS}} .

# Build and push the PR docker image with GoReleaser under a single explicit
# `<repository>:pr-<num>-<sha7>` tag. It never touches `:latest`, `:nightly`, a
# GitHub release or the Helm charts. Needs GORELEASER_KEY and a Docker login to
# the registry. Used by the `Build-PR-Image` workflow job.
#
# .goreleaser.pr.yml includes the canonical .goreleaser.yml, so the PR image has
# the same binaries and build tags as a release: kafka,nats,clickhouse,databricks,
# s3,pyroscope. It does not include `azure`, unlike `just docker-build`.
docker-build-pr image: (_assert-pr-image image)
    #!/usr/bin/env bash
    set -euo pipefail
    image='{{ image }}'
    export IMAGE_REPOSITORY="${image%:*}"
    export PR_IMAGE_TAG="${image##*:}"
    export IMAGE_PROVENANCE=mode=min
    # GITHUB_TOKEN is removed on purpose: a PR build must never reach the GitHub
    # releases API, even if a release step were enabled by mistake.
    env -u GITHUB_TOKEN -u GH_TOKEN \
        goreleaser release --nightly --clean --config .goreleaser.pr.yml --skip=archive,release

# Refuse any PR image reference that is not `<repository>:pr-<num>-<sha7>`, so a
# PR build cannot move `latest`, `nightly` or a release tag.
[private]
_assert-pr-image image:
    #!/usr/bin/env bash
    set -euo pipefail
    image='{{ image }}'
    last="${image##*/}"
    tag="${last#*:}"
    if [[ "$last" != *:* ]] || ! [[ "$tag" =~ ^pr-[0-9]+-[0-9a-f]{7}$ ]]; then
        echo "error: '$image' is not <repository>:pr-<num>-<sha7>" >&2
        exit 1
    fi

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

# Generate all CLI demo GIFs (starts a temporary server automatically).
# Run inside the demo shell: nix develop .#demo --command just generate-demo
generate-demo: (_generate-demo demos)

# Generate a single CLI demo GIF
# Usage: just generate-demo-only demo_numscript
generate-demo-only name: (_generate-demo name)

_generate-demo tapes:
    #!/usr/bin/env bash
    set -euo pipefail
    for tool in vhs grpcurl; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo "$tool not found: run inside the demo shell (nix develop .#demo)" >&2
            exit 1
        fi
    done
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
        --cluster-id demo \
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

# Regenerate the real TypeScript SDK and exercise it against Ledger HTTP handlers.
# Requires Speakeasy authentication; separate from default CI validation.
# Run inside the sdk shell: nix develop .#sdk --command just test-sdk-bigint
test-sdk-bigint:
    #!/usr/bin/env bash
    set -euo pipefail
    for tool in speakeasy node npm; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo "$tool not found: run inside the sdk shell (nix develop .#sdk)" >&2
            exit 1
        fi
    done
    sdk_dir="{{justfile_directory()}}/build/sdk-bigint"
    rm -rf "$sdk_dir"
    mkdir -p "$sdk_dir"
    speakeasy generate sdk --schema openapi.yml --out "$sdk_dir" --lang typescript --auto-yes
    cp tests/sdk/package.json tests/sdk/package-lock.json "$sdk_dir/"
    cp tests/sdk/bigint.ts "$sdk_dir/src/ledger-bigint-test.ts"
    npm --prefix "$sdk_dir" ci --ignore-scripts --no-audit --no-fund
    (cd "$sdk_dir" && node node_modules/typescript/bin/tsc)
    LEDGER_SDK_TEST="$sdk_dir/esm/ledger-bigint-test.js" go test -tags sdk ./internal/adapter/http -run ^TestBigintOperations$ -count=1 -timeout=2m
