set dotenv-load

pre-commit: generate generate-proto generate-sdk tidy lint lint-client
pc: pre-commit

lint:
    GOEXPERIMENT=jsonv2 golangci-lint run --fix --build-tags it,local --timeout 5m

lint-client:
    GOEXPERIMENT=jsonv2 golangci-lint run --fix --build-tags it,local --timeout 5m ./cmd/client/...

tidy:
    go mod tidy

# Build the application
build:
    GOEXPERIMENT=jsonv2 go build -o server ./cmd/server

# Build the client application
build-client:
    GOEXPERIMENT=jsonv2 go build -o client ./cmd/client

# Run the application locally (single node)
run:
    GOEXPERIMENT=jsonv2 go run . run --node-id 1 --bind-addr 127.0.0.1:8888 --wal-dir ./wal/node-1 --data-dir ./data/node-1

# Run the client application
run-client:
    GOEXPERIMENT=jsonv2 go run ./cmd/client

install-client:
    go build -o $GOPATH/bin/ledger-poc-client ./cmd/client
    #todo: make optional or configurable or whatever
    ledger-poc-client completion zsh > ~/.oh-my-zsh/custom/completions/_ledger-poc-client

# Run tests
test:
    GOEXPERIMENT=jsonv2 go test ./... -tags it,e2e

# Clean build artifacts
clean:
    rm -rf client server

clean-benchmarks-data:
    rm -rf build

generate:
    rm $(find ./internal -name '*_generated_test.go') || true
    rm $(find ./internal -name '*_generated.go') || true
    go generate ./...

# Generate SDK from OpenAPI specification using Speakeasy
generate-sdk:
    @echo "Generating SDK from openapi.yml using Speakeasy..."
    @nix develop --command speakeasy generate sdk \
        --lang go \
        --schema openapi.yml \
        --out ./pkg/client
    @echo "SDK generated in ./pkg/client"

# Clean generated SDK
clean-sdk:
    rm -rf pkg/client

# Generate gRPC code from protobuf files
generate-proto:
    @echo "Generating gRPC code from proto files..."
    rm $(find ./internal -name '*.pb.go') || true
    @protoc --go_out=. --go_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-grpc_out=. \
        --go-grpc_opt=module=github.com/formancehq/ledger-v3-poc \
        -I misc/proto \
        misc/proto/raft_transport.proto \
        misc/proto/ledger.proto

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
