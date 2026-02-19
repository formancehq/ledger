# Getting Started

## Prerequisites

- **Nix with Flakes enabled** (required) - provides Go 1.25+, Just, golangci-lint, protoc, and all other tools
- **direnv** - [Installation](https://direnv.net/) and [shell hook](https://direnv.net/docs/hook.html)

## Setup

```bash
# Clone the repository
git clone <repo-url> && cd ledger-v3-poc

# Generate flake.lock (first time only)
nix flake update

# Allow direnv to load the Nix environment (first time only)
direnv allow
```

After setup, `direnv` automatically loads the Nix development environment whenever you `cd` into the project directory.

## Project Structure

```
ledger-v3-poc/
├── cmd/                    # Entry points
│   ├── server/            # Main server
│   └── ledgerctl/         # CLI client (ledgerctl)
├── internal/               # Internal code (not exported)
│   ├── application/       # Application module (fx wiring, gRPC servers)
│   ├── raft/              # Raft implementation (single group)
│   ├── ctrl/              # Controller layer (transaction processing)
│   ├── service/           # Business services
│   │   ├── admission/     # Admission service (preload, AttributeLoader)
│   │   ├── attributes/    # Attribute types, U128 hashing, collision detection
│   │   ├── cache/         # FSM attribute cache (generation-based)
│   │   ├── commands/      # Raft command builders
│   │   ├── futures/       # Async futures for proposal results
│   │   ├── node/          # Raft node lifecycle and transport
│   │   ├── processing/    # Transaction/log processing
│   │   └── state/         # FSM state machine and snapshots
│   ├── compat/            # Compatibility layer (HTTP, JSON)
│   ├── storage/           # Storage layer (Pebble, spool, WAL)
│   ├── transport/         # gRPC transport and connection pool
│   ├── monitoring/        # Observability (OTLP, Pyroscope, trace sampling)
│   ├── proto/             # Generated protobuf types
│   └── crypto/            # Signing and keystore
├── misc/                   # Proto files, dev environment, demos
├── tests/                 # End-to-end tests
├── pkg/                    # Exported packages (testserver)
└── docs/                  # Documentation
```

## Build and Run

```bash
# Build the server and client
just build
just build-client

# Start a single node locally
just run

# Or manually
go run . run \
  --node-id 1 \
  --bind-addr 127.0.0.1:8888 \
  --data-dir ./data/node-1 \
  --http-port 9000
```

## Run Tests

```bash
# Unit tests
just test

# End-to-end tests
just test-e2e
```

## Development Workflow

1. Make your changes
2. Run pre-commit checks: `direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit`
3. Verify compilation: `go build ./...`
4. Run tests: `just test`

The pre-commit command runs `go generate ./...` (mocks + proto), `go mod tidy`, and `golangci-lint run --fix`.

## Dependency Injection with fx

The application uses Uber's `fx` for dependency injection, following the same patterns as `github.com/formancehq/ledger`.

### Module Pattern

Each module exports a `Module()` function returning `fx.Option`:

```go
func Module() fx.Option {
    return fx.Options(
        fx.Provide(NewMyComponent),
        fx.Invoke(StartMyComponent),
    )
}
```

### Lifecycle Management

Components register `OnStart`/`OnStop` hooks via `fx.Lifecycle`:

```go
func NewComponent(lc fx.Lifecycle) *Component {
    c := &Component{}
    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error { return c.Start(ctx) },
        OnStop:  func(ctx context.Context) error { return c.Stop(ctx) },
    })
    return c
}
```

### Application Startup

The server uses `github.com/formancehq/go-libs/v3/service` for lifecycle management:
- `service.Execute()` binds environment variables to flags (e.g., `NODE_ID` -> `--node-id`)
- `app.Run()` handles startup, signal handling (SIGTERM/SIGINT), and graceful shutdown

## Formance Libraries

| Library | Purpose |
|---------|---------|
| `go-libs/v3/service` | Application lifecycle, env-to-flag binding |
| `go-libs/v3/otlp` | OpenTelemetry configuration |
| `go-libs/v3/otlp/otlptraces` | Trace exporter configuration |
| `go-libs/v3/httpserver` | HTTP server lifecycle with `serverport` |

## Debugging

| Method | How |
|--------|-----|
| Debug logs | `DEBUG=true` environment variable |
| Tracing | Configure `OTEL_TRACES_EXPORTER_OTLP_ENDPOINT` |
| Profiling | `/debug/pprof/profile` endpoint or Pyroscope |

## Next Steps

- Read [conventions.md](./conventions.md) for coding standards
- Explore [architecture/overview.md](./architecture/overview.md) for the system design
- See [testing.md](./testing.md) for testing guidelines
