# Storage Sanity Check

## Overview

The sanity check feature allows operators to verify the integrity and consistency of the local storage on each node. Unlike most API endpoints that are forwarded to the Raft leader, the sanity check operates **locally** on each node, enabling node-specific diagnostics.

## Purpose

In a distributed system with Raft consensus, data is replicated across multiple nodes. While Raft guarantees consistency of committed entries, various issues can affect local storage:

- **Storage corruption**: Disk errors, filesystem issues, or hardware failures
- **Incomplete replication**: Network issues during snapshot transfers
- **Software bugs**: Storage driver issues or encoding/decoding problems
- **Manual intervention**: Accidental file modifications or deletions

The sanity check endpoint provides a way to detect these issues on individual nodes.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Cluster                                     │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │     Node 1      │  │     Node 2      │  │     Node 3      │     │
│  │    (Leader)     │  │   (Follower)    │  │   (Follower)    │     │
│  │                 │  │                 │  │                 │     │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │     │
│  │  │  Storage  │  │  │  │  Storage  │  │  │  │  Storage  │  │     │
│  │  │  (Pebble) │  │  │  │  (Pebble) │  │  │  │  (Pebble) │  │     │
│  │  └─────┬─────┘  │  │  └─────┬─────┘  │  │  └─────┬─────┘  │     │
│  │        │        │  │        │        │  │        │        │     │
│  │   Sanity Check  │  │   Sanity Check  │  │   Sanity Check  │     │
│  │   (Local Only)  │  │   (Local Only)  │  │   (Local Only)  │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Characteristics

- **No leader forwarding**: Unlike write operations, sanity checks are processed locally
- **Node-specific**: Each node can be checked independently
- **Non-blocking**: Sanity checks do not affect normal cluster operations
- **Read-only**: No modifications are made to the storage during checks

## API Endpoint

### Request

```http
GET /{ledgerName}/sanity-check
```

### Parameters

| Parameter | Type | Location | Description |
|-----------|------|----------|-------------|
| `ledgerName` | string | path | Name of the ledger to check |

### Response

```json
{
  "data": {
    "status": "ok"
  }
}
```

### Status Values

| Status | Description |
|--------|-------------|
| `ok` | Storage is consistent and valid |
| `error` | Storage has inconsistencies or errors |
| `not_implemented` | Sanity check logic is not yet implemented |

### HTTP Status Codes

| Code | Description |
|------|-------------|
| `200 OK` | Check completed (check `status` field for result) |
| `404 Not Found` | Ledger does not exist |
| `500 Internal Server Error` | Check could not be performed |

## CLI Usage

```bash
# Run sanity check on a specific ledger
ledger-poc-client ledgers sanity-check --name my-ledger

# With custom server URL
ledger-poc-client --server-url http://node2:9000 ledgers sanity-check --name my-ledger
```

### Example Output

```
✔ Sanity check completed

╭─────────────────╮
│ Result          │
├─────────────────┤
│ Ledger: my-ledger
│ Status: ok      │
╰─────────────────╯
```

## Use Cases

### 1. Routine Health Checks

Schedule periodic sanity checks on all nodes to detect issues early:

```bash
# Check all nodes in the cluster
for node in node1 node2 node3; do
  echo "Checking $node..."
  ledger-poc-client --server-url http://$node:9000 ledgers sanity-check --name production
done
```

### 2. Post-Deployment Verification

After deploying new nodes or recovering from failures, verify storage integrity:

```bash
# Verify a newly added node
ledger-poc-client --server-url http://new-node:9000 ledgers sanity-check --name production
```

### 3. Pre-Maintenance Checks

Before performing maintenance or upgrades, verify all nodes are healthy:

```bash
# Pre-maintenance checklist
for ledger in ledger1 ledger2 ledger3; do
  echo "Checking $ledger..."
  ledger-poc-client ledgers sanity-check --name $ledger
done
```

### 4. Incident Investigation

When investigating data issues, check individual nodes to isolate the problem:

```bash
# Check specific node that's suspected to have issues
ledger-poc-client --server-url http://suspect-node:9000 ledgers sanity-check --name production
```

## Planned Checks

The sanity check feature will verify the following aspects of local storage:

### Log Integrity

- **Sequential IDs**: Log entries have sequential IDs without gaps
- **Hash chain**: Each log entry's hash matches the computed hash
- **Payload validity**: Log payloads can be deserialized correctly

### Balance Consistency

- **Account balances**: Sum of all postings equals current balances
- **Asset totals**: Total amount of each asset is conserved across accounts

### Metadata Consistency

- **Account metadata**: Metadata matches the state from applied logs
- **Transaction metadata**: Transaction metadata is consistent with logs

### Index Verification

- **Idempotency keys**: All idempotency keys map to valid logs
- **Transaction IDs**: All transaction ID mappings are correct
- **Reference uniqueness**: Transaction references are unique

### Storage Backend Checks

Depending on the storage driver, additional checks may include:

**Pebble**:
- LSM tree integrity
- Block checksum verification
- Manifest file consistency

## Monitoring Integration

### Prometheus Metrics

The sanity check results can be exposed as metrics:

```
# Sanity check result (1 = ok, 0 = error)
ledger_sanity_check_status{ledger="production", node="node1"} 1

# Last sanity check timestamp
ledger_sanity_check_timestamp{ledger="production", node="node1"} 1706108400
```

### Alerting

Configure alerts for sanity check failures:

```yaml
# Prometheus alert rule
groups:
  - name: ledger-storage
    rules:
      - alert: LedgerStorageInconsistency
        expr: ledger_sanity_check_status == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Storage inconsistency detected on {{ $labels.node }}"
          description: "Ledger {{ $labels.ledger }} on node {{ $labels.node }} has storage inconsistencies"
```

## Best Practices

### Regular Checks

- Run sanity checks daily on all nodes
- Automate checks as part of your monitoring pipeline
- Keep historical records of check results

### Node-by-Node Verification

Since the sanity check is local, always check all nodes in the cluster:

```bash
#!/bin/bash
# check-all-nodes.sh
NODES="node1:9000 node2:9000 node3:9000"
LEDGER="production"

for node in $NODES; do
  result=$(ledger-poc-client --server-url http://$node ledgers sanity-check --name $LEDGER 2>&1)
  if echo "$result" | grep -q "Status: ok"; then
    echo "✓ $node: OK"
  else
    echo "✗ $node: FAILED"
    echo "$result"
  fi
done
```

### Handling Failures

When a sanity check fails:

1. **Isolate the node**: Remove it from the load balancer
2. **Investigate**: Check logs and storage state
3. **Decide recovery strategy**:
   - If recoverable: Fix the issue and re-run sanity check
   - If not recoverable: Rebuild the node from a healthy replica

### Recovery from Inconsistencies

If a node has storage inconsistencies:

1. **Stop the node**
2. **Clear local storage**
3. **Restart the node**
4. **Wait for snapshot transfer** from the leader
5. **Verify with sanity check**

## Limitations

- **Current implementation**: Returns `not_implemented` status (logic pending)
- **Point-in-time check**: Represents storage state at check time
- **No automatic repair**: Detection only, manual intervention required
- **Resource usage**: Full checks may be resource-intensive for large ledgers

## Related Documentation

- [Storage and Persistence](./storage.md) - Storage architecture details
- [Storage Drivers](./storage-drivers.md) - Driver-specific information
- [Deployment](./deployment.md) - Operational best practices
- [Metrics](./metrics.md) - Monitoring and alerting
