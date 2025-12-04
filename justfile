# Build the application
build:
    go build -o ledger-v3-poc ./cmd/server

# Run the application locally (single node)
run:
    go run ./cmd/server --node-id node-1 --bind-addr 127.0.0.1:8888 --data-dir ./data/node-1

# Run tests
test:
    go test ./...

# Clean build artifacts
clean:
    rm -f ledger-v3-poc
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

