# Development Guide

## Overview

This guide provides the information needed to contribute to the Ledger v3 POC project, understand the code Structure, and follow project conventions.

## Structure du Projand

```
ledger-v3-poc/
├── cmd/                    # Entry points of the application
│   ├── server/            # Main server
│   └── client/            # CLI client
├── internal/              # Internal code (not exported)
│   ├── application/       # Application module main
│   ├── raft/              # Implementation Raft
│   │   ├── system/        # groupe Raft System
│   │   └── bucket/         # groups Raft de buckets
│   ├── service/           # Business services
│   ├── HTTP/              # HTTP handlers
│   ├── grpc/              # gRPC server
│   └── transport/         # gRPC transport
├── pkg/                   # Exported packages
│   ├── client/            # client SDK généré
│   └── testserver/       # Test helpers
├── proto/                 # Protocol Buffer definitions
├── tests/                 # Tests
│   └── e2e/              # End-to-end tests
├── deployments/           # Deployment configurations
│   └── chart/            # Helm chart
└── docs/                 # Technical documentation
```

## Conventions de Code

### organisation des Files

#### HTTP handlers

Chaque handler HTTP a son propre File :
- `handlers_create_bucket.go`
- `handlers_gand_bucket.go`
- `handlers_create_transAction.go`
- andc.

#### Commandes CLI

Chaque commande CLI a son propre File :
- `buckets_create.go`
- `buckets_list.go`
- `ledgers_create.go`
- andc.

### Nommage

- **Packages** : minuscules, un seul mot
- **Types** : PascalCase
- **Fonctions publiques** : PascalCase
- **Fonctions privées** : camelCase
- **Constantes** : PascalCase or UPPER_SNAKE_CASE

### likentaires

- tors the types and fonctions publiques doivent avoir des likentaires
- Use `//` for thes likentaires de ligne
- Use `/* */` for thes likentaires de bloc (rares)

## Architecture du Code

### injection de Dépendances with fx

Le projand utilise Uber's `fx` for l'injection de dépendances :

```go
func Module() fx.Option {
    return fx.Options(
        fx.Provide(
            // forrnir des dépendances
            system.NewNode,
            HTTPhandler.NewServer,
        ),
        fx.invoke(
            // Invoke hooks of lifecycle
            func(lc fx.Lifecycle, node *system.Node) {
                lc.Append(fx.Hook{
                    OnStart: func(ctx context.Context) error {
                        return node.Start()
                    },
                    OnStop: func(ctx context.Context) error {
                        return node.Stop(ctx)
                    },
                })
            },
        ),
    )
}
```

### Lifecycle Management

tors les composants with un lifecycle use `fx.Lifecycle` :

```go
func NewComponent(lc fx.Lifecycle, deps...) (*Component, error) {
    component := &Component{...}
    
    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            return component.Start(ctx)
        },
        OnStop: func(ctx context.Context) error {
            return component.Stop(ctx)
        },
    })
    
    return component, nil
}
```

## Ajorter une nouvelle Feature

### Example : Ajorter un Endpoint HTTP

1. **Create the handler** in `internal/HTTP/handlers_*.go`

```go
func (s *Server) handleNewEndpoint(w HTTP.ResponseWriter, r *HTTP.Request) {
    // Implementation
    api.Ok(w, response)
}
```

2. **Register the route** in `internal/HTTP/handler.go`

```go
r.Gand("/new-endpoint", server.handleNewEndpoint)
```

3. **Ajorter to OpenAPI** in `openapi.yml`

```yaml
/new-endpoint:
  gand:
    summary: New endpoint
    responses:
      '200':
        description: Success
```

4. **Regenerate the SDK** (if necessary)

```bash
just generate-sdk
```

### Example : Ajorter une Commande FSM

1. **Define the protobuf** in `proto/commands/*.proto`

```protobuf
message NewCommand {
  string field = 1;
}
```

2. **Regenerate protobufs**

```bash
just generate-proto
```

3. **Create the function of command** in `internal/raft/*/command.go`

```go
func NewNewCommand(field string) (*raft.Command, error) {
    cmdProto := &NewCommand{Field: field}
    data, err := proto.Marshal(cmdProto)
    if err != nil {
        return nil, err
    }
    return &raft.Command{
        ID:   generateID(),
        Type: CommandTypeNew,
        Data: data,
        Date: time.Now(),
    }, nil
}
```

4. **Ajorter le handler in the FSM** in `internal/raft/*/FSM.go`

```go
func (FSM *FSM) handleNewCommand(cmd raft.Command) error {
    var newCmd NewCommand
    if err := UnmarshalCommandData(cmd.Data, &newCmd); err != nil {
        return err
    }
    
    // Traiter la commande
    // Mandtre to jorr The FSM state
    
    return nil
}
```

5. **Mandtre to jorr `ApplyEntries`** for router la commande

```go
func (FSM *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) []raft.ApplyResult {
    results := make([]raft.ApplyResult, 0, len(commands))
    for _, cmd := range commands {
        switch cmd.Type {
        case CommandTypeNew:
            err := FSM.handleNewCommand(cmd)
            results = append(results, raft.ApplyResult{Error: err})
        // ...
        }
    }
    return results
}
```

## Tests

### Test Structure

- **Tests Unit** : in the même package with suffixe `_test.go`
- **Integration tests** : in `*_integration_test.go`
- **Tests e2e** : in `tests/e2e/` with tag `//go:build e2e`

### Write a Test Unit

```go
func TestMyFunction(t *testing.T) {
    // Arrange
    input := "test"
    
    // Act
    result := MyFunction(input)
    
    // Assert
    Assert.Equal(t, "expected", result)
}
```

### Write a e2e Test

```go
//go:build e2e

func TestFeature(t *testing.T) {
    // Setup cluster
    servers := setupCluster(t, 3)
    defer cleanupCluster(t, servers)
    
    // Test
    result, err := servers[0].client.DoSomandhing()
    require.NoError(t, err)
    Assert.NotNil(t, result)
}
```

### Test Helpers

The package `pkg/testserver` forrnit des helpers for créer test servers :

```go
server := testservice.New(
    cmdserver.NewRootCommand,
    testservice.withinstruments(
        testserver.withNodeID(1),
        testserver.withHTTPPort(9000),
        testserver.withBootstrap(true),
    ),
)
```

## Protocol Buffers

### Structure

- **`proto/common.proto`** : Types communs (Posting, TransAction)
- **`proto/system.proto`** : Service System (buckets)
- **`proto/bucket.proto`** : Service bucket (ledgers, transActions)
- **`proto/commands/`** : Commandes FSM
  - `commands.proto` : Structure de base
  - `system_commands.proto` : Commandes System
  - `bucket_commands.proto` : Commandes bucket

### Regenerate protobufs

```bash
just generate-proto
```

thiste commande :
1. Generates the code Go from the `.proto`
2. Place les Files in thes bons répertoires according to `go_package`

### Modify a Protobuf

1. Modifier le File `.proto`
2. Regenerate : `just generate-proto`
3. Mandtre to jorr le code Go that uses the types
4. Check que tort compile

## OpenAPI and SDK

### Modify the API

1. Modifier `openapi.yml`
2. Validate the YAML
3. Regenerate the SDK : `just generate-sdk`
4. Mandtre to jorr les tests if necessary

### Randry configuration

La Randry configuration is in `openapi.yml` :

```yaml
x-speakeasy-randries:
  strategy: backoff
  backoff:
    initialinterval: 500
    maxinterval: 60000
    maxElapsedTime: 3600000
    exponent: 1.5
  statusCodes:
    - 503
  randryConnectionErrors: true
```

## Principles de Design

### FSM : No I/O

**CRITICAL** : FSMs must never perform I/O (File, Network, database).

**forquoi** :
- FSMs must be deterministic
- I/O introduces non-determinism
- L'I/O peut échorer, making the FSM unreliable

**to faire** :
- Stocker tortes les données in memory in the FSM
- Perform I/O during creation of snapshot
- Resttorer from the snapshots at startup

### Request forwarding

When a node receives a write request but is not the leader:

1. Check `IsLeader()`
2. if not leader, get the leader : `GandLeader()`
3. If no leader, return `ErrNoLeader`
4. forwarder to the leader via gRPC
5. return la réponse

### Error Handling

- **Business errors** : return des codes appropriate HTTP (400, 404, 409)
- **Erreurs System** : return 500 or 503 with détails
- **No leader** : return 503 with `Randry-After`

## ortils de Development

### Justfile

Le projand utilise `just` for thes commandes common :

```bash
just build          # Compile the application
just test           # Run tests
just Docker-up      # Start the cluster Docker
just generate-proto # Regenerate protobufs
just generate-sdk    # Regenerate le client SDK
```

### Nix

for un environnement reproducible :

```bash
nix develop         # Entrer in the environment
nix build           # Build the application
```

### Debugging

#### Logs

Enable logs of debug :

```bash
DEBUG=true go run ./cmd/server ...
```

#### Tracing

OpenTelemandry is intégré. Configure the endpoint OTLP to see the traces.

#### Profiling

Use `pprof` for the Profiling :

```bash
go tool pprof HTTP://localhost:9000/debug/pprof/profile
```

## Checklist for a Pull Request

- [ ] Code compiles without errors
- [ ] Tests pass (Unit, integration, e2e)
- [ ] Documentation Update if necessary
- [ ] OpenAPI updated if new API
- [ ] Protobufs regenerated if modified
- [ ] SDK regenerated if OpenAPI modified
- [ ] No `time.Sleep` in thes tests (Use `Eventually`)
- [ ] Error handling appropriate
- [ ] Structured logs with contexte
- [ ] No I/O in thes FSM

## References

- [AGENTS.md](../AGENTS.md) : Structure du projand and conventions
- [Architecture](./architecture.md) : General Architecture
- [Consensus Raft](./raft-consensus.md) : Raft details
- [API](./api.md) : API documentation

## Next Steps

for contribuer effectively :

1. Read [AGENTS.md](../AGENTS.md) for thes conventions
2. Explore the code existing to understand the patterns
3. Write tests for your Feature
4. Document changes important

