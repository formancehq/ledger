set dotenv-load

default:
  @just --list

pre-commit: generate earthly tidy lint export-docs-events

earthly:
  @earthly --no-output --secret SPEAKEASY_API_KEY=$SPEAKEASY_API_KEY +pre-commit

lint:
  @golangci-lint run --fix --build-tags it --timeout 5m
  @cd {{justfile_directory()}}/tools/generator && golangci-lint run --fix --build-tags it --timeout 5m
  @cd {{justfile_directory()}}/deployments/pulumi && golangci-lint run --fix --build-tags it --timeout 5m

tidy:
  @go mod tidy
  @cd {{justfile_directory()}}/tools/generator && go mod tidy
  @cd {{justfile_directory()}}/deployments/pulumi && go mod tidy

generate:
  @go generate ./...

export-docs-events:
  @go run . docs events --write-dir docs/events

tests:
  @go test -race -covermode=atomic \
    -coverpkg=github.com/formancehq/ledger/internal/...,github.com/formancehq/ledger/pkg/events/...,github.com/formancehq/ledger/pkg/accounts/...,github.com/formancehq/ledger/pkg/assets/...,github.com/formancehq/ledger/cmd/... \
    -coverprofile coverage.txt \
    -tags it \
    ./...
  @cat coverage.txt | grep -v debug.go | grep -v "/machine/" > coverage2.txt
  @mv coverage2.txt coverage.txt

release-local:
  @goreleaser release --nightly --skip=publish --clean

release-ci:
  @goreleaser release --nightly --clean

release:
  @goreleaser release --clean
