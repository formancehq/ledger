set dotenv-load

default:
  @just --list

pre-commit: generate tidy lint export-docs-events openapi generate-client
pc: pre-commit

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

openapi:
  @yq eval-all '. as $item ireduce ({}; . * $item)' openapi/v1.yaml openapi/v2.yaml openapi/overlay.yaml > openapi.yaml
  @npx -y widdershins {{justfile_directory()}}/openapi/v2.yaml -o {{justfile_directory()}}/docs/api/README.md --search false --language_tabs 'http:HTTP' --summary --omitHeader

generate-client:
  @speakeasy generate sdk -s openapi.yaml -o ./pkg/client -l go

release-local:
  @goreleaser release --nightly --skip=publish --clean

release-ci:
  @goreleaser release --nightly --clean

release:
  @goreleaser release --clean
