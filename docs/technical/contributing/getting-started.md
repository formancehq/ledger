# Getting Started

## Prerequisites

- **Nix with Flakes enabled** (required) - provides Go 1.26+, Just, golangci-lint, protoc, Atlassian CLI (`acli`), and all other repository CLI dependencies
- **direnv** - [Installation](https://direnv.net/) and [shell hook](https://direnv.net/docs/hook.html)

## Setup

```bash
# Clone the repository
git clone <repo-url> && cd ledger

# Generate flake.lock (first time only)
nix flake update

# Allow direnv to load the Nix environment (first time only)
direnv allow
```

After setup, `direnv` automatically loads the Nix development environment whenever you `cd` into the project directory.

Repository scripts and contributor workflows must not rely on host-installed CLIs. When adding a CLI dependency, add it to `flake.nix` so its version is pinned by `flake.lock`. External service authentication, such as `acli` credentials, remains an explicit operator setup step and is not managed by Nix.

## Project Structure

```
ledger/
├── main.go                    # Entry point
├── cmd/
│   ├── server/                # Server command
│   └── ledgerctl/             # CLI tool (sub-packages per command)
├── internal/
│   ├── adapter/               # Transport layer (grpc/, http/, json/, auth/, v2/)
│   ├── application/           # Use cases (admission/, ctrl/, events/, check/, indexbuilder/, mirror/)
│   ├── bootstrap/             # Composition root (fx wiring, config, TLS)
│   ├── domain/                # Business domain (processing/, crypto/, accounttype/, analysis/, replay/)
│   ├── infra/                 # Infrastructure (node/, state/, cache/, attributes/, transport/, health/, monitoring/, backup/, bloom/, preload/)
│   ├── pkg/                   # Internal utilities (kv/, signal/, futures/, commands/, bitset/, bytesize/, filterexpr/, semver/, tarutil/, vtmarshal/, worker/)
│   ├── proto/                 # Generated protobuf code
│   ├── query/                 # CQRS read-side queries
│   └── storage/               # Pebble persistence (dal/, wal/, spool/, readstore/, pebblecfg/)
├── pkg/                       # Public packages (actions/, scenario/, testserver/)
├── tests/                     # Test suites (e2e/, scenarios/, antithesis/, perf/, schemathesis/)
├── misc/
│   ├── proto/                 # Protocol Buffer definitions
│   ├── demo/                  # VHS tape files for CLI demos
│   ├── numscript/examples/    # Numscript examples
│   └── devenv/                # Development environment
└── docs/                      # Documentation
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
  --cluster-id local-dev \
  --bootstrap \
  --bind-addr 127.0.0.1:7777 \
  --grpc-port 8888 \
  --wal-dir ./wal/node-1 \
  --data-dir ./data/node-1
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
2. Run the fast baseline: `bash scripts/agent-check`
3. Run focused tests for the changed package or subsystem
4. Before publishing a PR candidate, run `AI_REVIEW_BASE_SHA=<base-sha> bash scripts/agent-check-pr`

The PR command selects applicable normalization and focused tests from the
exact diff. Use `bash scripts/agent-check-full` only as an explicit broad
fallback. See [Local validation](local-validation.md).

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

The server uses `github.com/formancehq/go-libs/v5/pkg/service` for lifecycle management:
- `service.Execute()` binds environment variables to flags (e.g., `NODE_ID` -> `--node-id`)
- `service.NewWithLogger(logger, opts...).Run(cmd)` handles startup, signal handling (SIGTERM/SIGINT), and graceful shutdown

## Formance Libraries

| Library | Purpose |
|---------|---------|
| `go-libs/v5/pkg/service` | Application lifecycle, env-to-flag binding |
| `go-libs/v5/pkg/observe` | OpenTelemetry configuration |
| `go-libs/v5/pkg/observe/traces` | Trace exporter configuration |
| `go-libs/v5/pkg/transport/httpserver` | HTTP server lifecycle with `serverport` |

## Debugging

| Method | How |
|--------|-----|
| Debug logs | `DEBUG=true` environment variable |
| Tracing | Configure `OTEL_TRACES_EXPORTER_OTLP_ENDPOINT` |
| Profiling | `/debug/pprof/profile` endpoint or Pyroscope |

## Next Steps

- Read [conventions.md](./conventions.md) for coding standards
- Explore [architecture/overview.md](../architecture/overview.md) for the system design
- See [testing.md](./testing.md) for testing guidelines
