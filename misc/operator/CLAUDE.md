# Ledger Operator

## Development Environment

All commands (build, test, lint, etc.) MUST be run through `nix develop` to ensure correct toolchain versions. Use:

```sh
nix develop --command <cmd>
```

For example:
```sh
nix develop --command go build ./...
nix develop --command go test ./...
nix develop --command golangci-lint run
```
