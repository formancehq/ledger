# Enterprise Edition (EE) Features

## Audit Logging

### Configuration

Audit logging requires a publisher to be configured (Kafka or NATS).

#### Minimal Configuration

```bash
ledger-ee serve \
  --publisher-kafka-enabled=true \
  --publisher-kafka-broker=localhost:9092 \
  --audit-enabled=true
```

#### Production Configuration (Recommended)

**Important**: Audit automatically uses `{stack}.audit` when you configure a wildcard mapping.

```bash
# With NATS (audit automatically goes to mystream.audit)
ledger-ee serve \
  --publisher-nats-enabled=true \
  --publisher-nats-url=nats://nats:4222 \
  --publisher-topic-mapping=*:mystream.ledger \
  --audit-enabled=true

# With Kafka (audit automatically goes to ledger.audit)
ledger-ee serve \
  --publisher-kafka-enabled=true \
  --publisher-kafka-broker=kafka:9092 \
  --publisher-topic-mapping=*:ledger.events \
  --audit-enabled=true
```

### Topic Mapping

**Automatic Topic Detection**: Audit automatically extracts the stack name from wildcard topic mappings and publishes to `{stack}.audit`.

| Wildcard Mapping | Audit Topic | Ledger Events Topic |
|-----------------|-------------|---------------------|
| `*:stack.ledger` | `stack.audit` ✅ **Auto-detected** | `stack.ledger` |
| `*:example-toto.ledger` | `example-toto.audit` ✅ **Auto-detected** | `example-toto.ledger` |
| `*:prod.us-east.ledger` | `prod.us-east.audit` ✅ **Auto-detected** | `prod.us-east.ledger` |
| No mapping | `AUDIT` | `COMMITTED_TRANSACTIONS`, etc. |

**Manual Override** (optional): You can explicitly set the audit topic if needed:
```bash
--publisher-topic-mapping=AUDIT:custom.audit --publisher-topic-mapping=*:stack.ledger
```

### Event Types Published

**Audit Events**: `AUDIT`
- HTTP request/response details
- Identity (from JWT)
- Timestamp, status code, headers, body (limited by `--audit-max-body-size`)

**Ledger Events**: `COMMITTED_TRANSACTIONS`, `SAVED_METADATA`, `REVERTED_TRANSACTION`, `DELETED_METADATA`

### Configuration Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--audit-enabled` | `false` | Enable audit logging (EE only) |
| `--audit-max-body-size` | `1048576` (1MB) | Max request/response body size to capture |
| `--audit-excluded-paths` | `/_healthcheck`, `/_/healthcheck` | Paths to exclude from audit |
| `--audit-sensitive-headers` | `Authorization`, `Cookie`, `X-API-Key` | Headers to sanitize |

### Example: Production Setup

```bash
# Environment variables
export PUBLISHER_NATS_ENABLED=true
export PUBLISHER_NATS_URL=nats://nats.prod:4222
export PUBLISHER_TOPIC_MAPPING=*:prod-ledger.events  # Audit auto-goes to prod-ledger.audit
export AUDIT_ENABLED=true
export AUDIT_MAX_BODY_SIZE=2097152  # 2MB
export AUDIT_EXCLUDED_PATHS=/_healthcheck,/_/healthcheck,/_/metrics

# Start ledger (audit automatically publishes to prod-ledger.audit)
ledger-ee serve
```

### Audit Event Format

```json
{
  "date": "2025-01-06T10:30:00Z",
  "app": "ledger",
  "version": "v1",
  "type": "AUDIT",
  "payload": {
    "id": "uuid-here",
    "identity": "user@example.com",
    "request": {
      "method": "POST",
      "path": "/v2/transactions",
      "host": "ledger.example.com",
      "header": {
        "Content-Type": ["application/json"],
        "Authorization": ["[REDACTED]"]
      },
      "body": "{\"postings\":[...]}"
    },
    "response": {
      "status_code": 201,
      "headers": {...},
      "body": "{\"data\":{...}}"
    }
  }
}
```

## Building

```bash
# Community Edition (no audit)
go build -o ledger .

# Enterprise Edition (with audit)
go build -tags=ee -o ledger-ee .
```
