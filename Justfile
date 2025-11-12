set dotenv-load

default:
  @just --list

pre-commit: tidy generate lint export-docs-events openapi generate-client
pc: pre-commit

lint:
    golangci-lint --version
    golangci-lint run --fix --build-tags it,local --timeout 5m
    for d in $(ls tools); do \
        pushd tools/$d; \
        golangci-lint run --fix --build-tags it --timeout 5m; \
        popd; \
    done
    cd {{justfile_directory()}}/deployments/pulumi && golangci-lint run --fix --build-tags it --timeout 5m

tidy:
    for d in $(ls tools); do \
        pushd tools/$d; \
        go mod tidy; \
        popd; \
    done
    go mod tidy
    cd {{justfile_directory()}}/deployments/pulumi && go mod tidy

generate:
    rm $(find ./internal -name '*_generated_test.go') || true
    go generate ./...
g: generate

export-docs-events:
    go run . docs events --write-dir docs/events

tests:
    go test -race -covermode=atomic \
        -coverpkg=github.com/formancehq/ledger/internal/...,github.com/formancehq/ledger/pkg/events/...,github.com/formancehq/ledger/pkg/accounts/...,github.com/formancehq/ledger/pkg/assets/...,github.com/formancehq/ledger/cmd/... \
        -coverprofile coverage.txt \
        -tags it \
        ./...
    cat coverage.txt | grep -v debug.go | grep -v "/machine/" | grep -v "pb.go" > coverage2.txt
    mv coverage2.txt coverage.txt

fmt:
  @golangci-lint fmt

openapi:
    yq eval-all '. as $item ireduce ({}; . * $item)' openapi/v1.yaml openapi/v2.yaml openapi/overlay.yaml > openapi.yaml
    npx -y widdershins {{justfile_directory()}}/openapi/v2.yaml -o {{justfile_directory()}}/docs/api/README.md --search false --language_tabs 'http:HTTP' --summary --omitHeader

generate-client: openapi
    if [ ! -z "$SPEAKEASY_API_KEY" ]; then cd pkg/client && speakeasy run --skip-versioning; fi

release-local:
    @goreleaser release --nightly --skip=publish --clean

release-ci:
    @goreleaser release --nightly --clean

release:
    @goreleaser release --clean

generate-grpc-replication:
    protoc --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        ./internal/replication/grpc/replication_service.proto
