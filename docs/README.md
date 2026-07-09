# Ledger v3 Documentation

Formance Ledger v3 is a distributed, high-performance financial ledger built on Raft consensus with embedded Pebble storage. Single binary, no external dependencies, 106K+ transactions per second.

## Documentation

### [Operations Guide](./ops/)
Deploy, monitor, and run the ledger in production. Covers deployment, CLI reference, cluster management, backup/restore, monitoring, authentication, and security.

### [Product Overview](./sales/)
Features, performance benchmarks, and key differentiators. For understanding what Ledger v3 offers and how it compares to v2.

### [Technical Documentation](./technical/)
Architecture, internals, and contributor guides. Start with the [Architecture Overview](./technical/architecture-overview.md) for the 10,000-foot view, then dive into specific topics.

### [Operator](../misc/operator/README.md)
Kubernetes operator for deploying and managing high-availability Ledger clusters. Manages `Cluster`, `Backup`, and `Credentials` custom resources. Includes a `kubectl ledger` plugin and a web UI.

### [Design RFCs](./drafts/)
Experimental ideas, advanced concepts, and future design proposals.

---

## Quick Links

| Topic | Link |
|-------|------|
| CLI reference | [ops/cli.md](./ops/cli.md) |
| Deployment guide | [ops/deployment.md](./ops/deployment.md) |
| Architecture overview | [technical/architecture-overview.md](./technical/architecture-overview.md) |
| API comparison (v2 parity) | [technical/contributing/api-comparison.md](./technical/contributing/api-comparison.md) |
| Benchmarks (106K tx/s) | [sales/benchmarks.md](./sales/benchmarks.md) |
| Getting started | [technical/contributing/getting-started.md](./technical/contributing/getting-started.md) |
| Operator | [misc/operator/README.md](../misc/operator/README.md) |
