# Ledger v3 Documentation

Formance Ledger v3 is a distributed, high-performance financial ledger built on Raft consensus with embedded Pebble storage. Single binary, no external dependencies, 106K+ transactions per second.

## Documentation authority

Documentation has different roles. For engineering work, do not treat every directory as equally authoritative.

1. **`technical/`** — authoritative engineering architecture, internals, and contributor guidance.
2. **`ops/`** — authoritative operational behavior, deployment, CLI, backup/restore, monitoring, and security guidance.
3. **`sales/`** — product-facing descriptions and benchmarks. Useful for product context, but not an engineering specification.
4. **`drafts/`** — experimental ideas and future design proposals. Non-authoritative unless a task explicitly targets that RFC/design.

AI agents should start from [`AGENTS.md`](../AGENTS.md) and use [`technical/agent-context.md`](./technical/agent-context.md) to load only task-relevant documentation.

## Documentation

### [Operations Guide](./ops/)
Deploy, monitor, and run the ledger in production. Covers deployment, CLI reference, cluster management, backup/restore, monitoring, authentication, and security.

### [Product Overview](./sales/)
Features, performance benchmarks, and key differentiators. For understanding what Ledger v3 offers and how it compares to v2. Product-facing and non-normative for implementation details.

### [Technical Documentation](./technical/)
Architecture, internals, and contributor guides. Start with the [Architecture Overview](./technical/architecture/overview.md) for the 10,000-foot view, then dive into specific topics.

### [Operator](../misc/operator/README.md)
Kubernetes operator for deploying and managing high-availability Ledger clusters. Manages `Cluster`, `Backup`, and `Credentials` custom resources. Includes a `kubectl ledger` plugin and a web UI.

### [Design RFCs](./drafts/)
Experimental ideas, advanced concepts, and future design proposals. These documents are non-authoritative unless a task explicitly asks to implement or evaluate a specific draft.

---

## Quick Links

| Topic | Link |
|-------|------|
| Agent context routing | [technical/agent-context.md](./technical/agent-context.md) |
| CLI reference | [ops/cli.md](./ops/cli.md) |
| Deployment guide | [ops/deployment-profiles.md](./ops/deployment-profiles.md) |
| Architecture overview | [technical/architecture/overview.md](./technical/architecture/overview.md) |
| API comparison (v2 parity) | [technical/contributing/api-comparison.md](./technical/contributing/api-comparison.md) |
| Query filtering (QueryFilter) | [technical/architecture/subsystems/read-path/query-filter.md](./technical/architecture/subsystems/read-path/query-filter.md) |
| Benchmarks (106K tx/s) | [sales/benchmarks.md](./sales/benchmarks.md) |
| Getting started | [technical/contributing/getting-started.md](./technical/contributing/getting-started.md) |
| Operator | [misc/operator/README.md](../misc/operator/README.md) |
