set dotenv-load

pre-commit: generate generate-proto tidy lint
pc: pre-commit

lint:
    GOEXPERIMENT=jsonv2 golangci-lint run --fix --build-tags it,local --timeout 5m

tidy:
    go mod tidy

# Build the server application
build:
    GOEXPERIMENT=jsonv2 go build -o ledger-v3-poc .

# Build the client application
build-client:
    GOEXPERIMENT=jsonv2 go build -o ledgerctl ./cmd/client

# Run the application locally (single node)
run:
    GOEXPERIMENT=jsonv2 go run . run --node-id 1 --bind-addr 127.0.0.1:8888 --wal-dir ./wal/node-1 --data-dir ./data/node-1

# Run the client application
run-client *ARGS:
    GOEXPERIMENT=jsonv2 go run ./cmd/client {{ARGS}}

install-client:
    go build -o $GOPATH/bin/ledgerctl ./cmd/client
    #todo: make optional or configurable or whatever
    ledgerctl completion zsh > ~/.oh-my-zsh/custom/completions/_ledgerctl

# Run tests
test:
    GOEXPERIMENT=jsonv2 go test ./... -tags it,e2e

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
    rm -f internal/proto/rafttransportpb/*.pb.go internal/proto/commonpb/*.pb.go internal/proto/servicepb/*.pb.go internal/proto/raftcmdpb/*.pb.go internal/proto/snapshotpb/*.pb.go internal/proto/clusterpb/*.pb.go || true
    mkdir -p internal/proto/clusterpb internal/proto/rafttransportpb
    @protoc --go_out=. --go_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-grpc_out=. \
        --go-grpc_opt=module=github.com/formancehq/ledger-v3-poc \
        -I misc/proto \
        misc/proto/raft_transport.proto \
        misc/proto/common.proto \
        misc/proto/cluster.proto \
        misc/proto/service.proto \
        misc/proto/raftcmd.proto \
        misc/proto/snapshot.proto

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
