# Build the application
build:
    go build -o ledger-v3-poc ./cmd/server

# Build the client application
build-client:
    go build -o ledger-client ./cmd/client

# Run the application locally (single node)
run:
    go run ./cmd/server --node-id node-1 --bind-addr 127.0.0.1:8888 --data-dir ./data/node-1

# Run the client application
run-client:
    go run ./cmd/client

# Run tests
test:
    go test ./...

# Clean build artifacts
clean:
    rm -f ledger-v3-poc ledger-client
    rm -rf data/

# Clean data directories (removes all node data)
clean-data:
    rm -rf data/node-1/*
    rm -rf data/node-2/*
    rm -rf data/node-3/*
    @echo "Data directories cleaned"

# Start Docker cluster
docker-up:
    docker-compose up -d

# Stop Docker cluster
docker-down:
    docker-compose down -v

# View logs
docker-logs:
    docker-compose logs -f

# Generate SDK from OpenAPI specification using Speakeasy
generate-sdk:
    @echo "Generating SDK from openapi.yml using Speakeasy..."
    @mkdir -p pkg/client
    @echo "go:" > pkg/client/gen.yaml
    @echo "  packageName: github.com/formancehq/ledger-v3-poc/pkg/client" >> pkg/client/gen.yaml
    @echo "  version: 1.0.0" >> pkg/client/gen.yaml
    @nix develop --command speakeasy generate sdk \
        --lang go \
        --schema openapi.yml \
        --out ./pkg/client
    @echo "SDK generated in ./pkg/client"

# Clean generated SDK
clean-sdk:
    rm -rf pkg/client

# Wait for a node to be healthy (helper function)
wait-for-healthy NODE:
    @./scripts/wait-for-healthy.sh {{NODE}}

# Rolling upgrade: upgrade nodes one by one to ensure cluster availability
rolling-upgrade:
    @echo "Starting rolling upgrade of Raft cluster..."
    @echo "Upgrading node-1..."
    @docker-compose stop node-1
    @docker-compose up -d node-1
    @just wait-for-healthy node-1
    @echo "node-1 upgraded and healthy"
    @echo ""
    @echo "Upgrading node-2..."
    @docker-compose stop node-2
    @docker-compose up -d node-2
    @just wait-for-healthy node-2
    @echo "node-2 upgraded and healthy"
    @echo ""
    @echo "Upgrading node-3..."
    @docker-compose stop node-3
    @docker-compose up -d node-3
    @just wait-for-healthy node-3
    @echo "node-3 upgraded and healthy"
    @echo ""
    @echo "Rolling upgrade completed successfully!"

