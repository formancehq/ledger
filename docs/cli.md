# CLI Reference (ledgerctl)

`ledgerctl` is the command-line client for interacting with Ledger v3 servers via gRPC.

## Getting Started

![Getting Started](../misc/demo/demo_getting_started.gif)

## Installation

```bash
# Build from source
just build-client

# Or directly with Go
go build -o build/ledgerctl ./cmd/ledgerctl
```

## Global Flags

These flags are available for all commands:

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | `localhost:8888` | gRPC server address |
| `--insecure` | `false` | Use insecure connection (no TLS) |

## Commands

### ledgers

Manage ledgers in the cluster.

**Aliases:** `ledger`, `lg`

#### ledgers list

List all ledgers in the cluster.

**Aliases:** `ls`, `l`

```bash
ledgerctl ledgers list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# List all ledgers
ledgerctl ledgers list

# Output as JSON
ledgerctl ledgers list --json
```

#### ledgers get

Get detailed information about a ledger.

**Aliases:** `g`, `show`, `describe`

```bash
ledgerctl ledgers get <name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
ledgerctl ledgers get my-ledger
```

#### ledgers create

Create a new ledger.

**Aliases:** `new`, `add`

```bash
ledgerctl ledgers create [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | | Name of the ledger to create |
| `--metadata` | | Metadata key=value pairs |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Create a ledger with a name
ledgerctl ledgers create --name my-ledger

# Create with metadata
ledgerctl ledgers create --name my-ledger --metadata description="My ledger" --metadata env=prod

# Interactive mode (will prompt for name)
ledgerctl ledgers create
```

#### ledgers delete

Delete a ledger (soft-delete).

**Aliases:** `rm`, `del`, `remove`

```bash
ledgerctl ledgers delete [name] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | | Name of the ledger to delete |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Delete a ledger (will prompt for confirmation)
ledgerctl ledgers delete my-ledger

# Delete without confirmation
ledgerctl ledgers delete my-ledger -y

# Interactive mode (will prompt for ledger selection)
ledgerctl ledgers delete
```

---

### accounts

![Metadata Demo](../misc/demo/demo_metadata.gif)

Manage accounts in a ledger.

**Aliases:** `account`, `acc`, `a`

#### accounts get

Get detailed information about an account including its volumes.

**Aliases:** `g`, `show`, `describe`

```bash
ledgerctl accounts get [address] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- If `--ledger` is not provided and only one ledger exists, it will be used automatically
- If multiple ledgers exist, you will be prompted to select one
- If address is not provided, you will be prompted to enter it

**Example:**

```bash
# Get account with explicit ledger
ledgerctl accounts get bank --ledger my-ledger

# Auto-select ledger if only one exists
ledgerctl accounts get bank

# Interactive mode
ledgerctl accounts get
```

#### accounts set-metadata

Set metadata on an account.

**Aliases:** `set-meta`, `sm`

```bash
ledgerctl accounts set-metadata [address] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `-m, --metadata` | | Metadata key=value pairs (can be repeated) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Set single metadata
ledgerctl accounts set-metadata bank --ledger my-ledger --metadata type=asset

# Set multiple metadata
ledgerctl accounts set-metadata users:alice -m role=admin -m tier=premium

# Interactive mode (will prompt for metadata)
ledgerctl accounts set-metadata bank --ledger my-ledger
```

#### accounts delete-metadata

Delete a metadata key from an account.

**Aliases:** `del-meta`, `dm`, `rm-meta`

```bash
ledgerctl accounts delete-metadata [address] [key] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Delete metadata key
ledgerctl accounts delete-metadata bank type --ledger my-ledger

# Delete without confirmation
ledgerctl accounts delete-metadata users:alice role -y

# Interactive mode
ledgerctl accounts delete-metadata
```

---

### transactions

![Transactions Demo](../misc/demo/demo_transactions.gif)

Manage transactions in a ledger.

**Aliases:** `transaction`, `tx`, `t`

#### transactions list

List transactions in a ledger with pagination.

**Aliases:** `ls`, `l`

```bash
ledgerctl transactions list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--page-size` | `10` | Number of transactions per page |
| `--all` | `false` | Fetch all transactions at once (no pagination) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Transactions are displayed **newest first**
- Interactive pagination: press Enter to load the next page, or 'q' to quit
- In JSON mode, only the first page is output (no interactive pagination)

**Example:**

```bash
# List transactions with interactive pagination
ledgerctl transactions list --ledger my-ledger

# Custom page size
ledgerctl transactions list --ledger my-ledger --page-size 20

# Fetch all transactions at once
ledgerctl transactions list --ledger my-ledger --all

# Output as JSON
ledgerctl transactions list --ledger my-ledger --json

# Interactive mode (will prompt for ledger selection)
ledgerctl transactions list
```

#### transactions get

Get detailed information about a transaction.

**Aliases:** `g`, `show`, `describe`

```bash
ledgerctl transactions get [transaction-id] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Get transaction by ID
ledgerctl transactions get 42 --ledger my-ledger

# Interactive mode
ledgerctl transactions get
```

#### transactions create

Create a new transaction.

**Aliases:** `new`, `add`

```bash
ledgerctl transactions create [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--posting` | | Posting in format: `source,destination,amount,asset` (can be repeated) |
| `--script` | | Path to a Numscript file (mutually exclusive with `--posting`) |
| `--var` | | Script variable in format: `name=value` (can be repeated, only with `--script`) |
| `--reference` | | Transaction reference |
| `--metadata` | | Metadata key=value pairs |
| `--force` | `false` | Bypass balance checks (allow accounts to go negative) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Creating transactions with postings:**

```bash
# Single posting
ledgerctl transactions create --ledger my-ledger --posting "world,bank,1000,USD"

# Multiple postings
ledgerctl transactions create --ledger my-ledger \
  --posting "world,bank,1000,USD" \
  --posting "bank,user,500,USD"

# With reference and metadata
ledgerctl transactions create --ledger my-ledger \
  --posting "world,bank,1000,USD" \
  --reference "order-123" \
  --metadata type=deposit --metadata source=api

# Force transaction (bypass balance check, allow negative balance)
ledgerctl transactions create --ledger my-ledger \
  --posting "empty-account,destination,1000,USD" \
  --force
```

**Creating transactions with Numscript:**

```bash
# Simple script
ledgerctl transactions create --ledger my-ledger \
  --script transfer.num \
  --var "source=users:alice" \
  --var "destination=users:bob" \
  --var "amount=USD/2 100"

# Using example scripts
ledgerctl transactions create --ledger my-ledger \
  --script numscript/examples/world_funding.num \
  --var "destination=bank" \
  --var "amount=USD/2 10000"
```

**Interactive mode:**

```bash
# Will prompt for ledger and postings
ledgerctl transactions create

# With script - will prompt for missing variables
ledgerctl transactions create --ledger my-ledger \
  --script numscript/examples/simple_transfer.num
# -> Prompts for: $source (account), $destination (account), $amount (monetary)

# Partial variables - will prompt only for missing ones
ledgerctl transactions create --ledger my-ledger \
  --script numscript/examples/simple_transfer.num \
  --var "source=users:alice"
# -> Prompts for: $destination (account), $amount (monetary)
```

**Interactive variable prompting:**

When using `--script`, the CLI parses the Numscript file and detects required variables. For any variable not provided via `--var`, it will interactively prompt you with the expected type and format hints:

- **account**: e.g., `users:alice`, `merchants:shop`
- **monetary**: e.g., `USD/2 1000`, `EUR/2 50`
- **string**: e.g., `order-123`, `ref-abc`
- **number**: e.g., `42`, `100`
- **portion**: e.g., `1/4`, `25%`, `0.25`

#### transactions revert

Revert a transaction by creating a counter-transaction that reverses all postings.

**Aliases:** `undo`, `reverse`

```bash
ledgerctl transactions revert [transaction-id] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--force` | `false` | Force revert even if funds have been spent |
| `--at-effective-date` | `false` | Use the original transaction timestamp for the revert |
| `--metadata` | | Metadata for the revert transaction (key=value) |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Creates a new transaction that reverses all postings from the original
- By default, prompts for confirmation before reverting
- Use `-y` or `--yes` to skip the confirmation prompt
- Use `--force` to revert even if funds have already been spent from receiving accounts

**Example:**

```bash
# Revert a transaction (will prompt for confirmation)
ledgerctl transactions revert 42 --ledger my-ledger

# Force revert even if funds have been spent
ledgerctl transactions revert 42 --ledger my-ledger --force

# Revert at the original transaction timestamp
ledgerctl transactions revert 42 --ledger my-ledger --at-effective-date

# Skip confirmation prompt
ledgerctl transactions revert 42 --ledger my-ledger -y

# Add metadata to the revert transaction
ledgerctl transactions revert 42 --ledger my-ledger \
  --metadata reason="customer refund" \
  --metadata ticket="JIRA-123"

# Interactive mode (will prompt for ledger and transaction ID)
ledgerctl transactions revert
```

#### transactions set-metadata

Set metadata on a transaction.

**Aliases:** `set-meta`, `sm`

```bash
ledgerctl transactions set-metadata [transaction-id] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `-m, --metadata` | | Metadata key=value pairs (can be repeated) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Set single metadata
ledgerctl transactions set-metadata 42 --ledger my-ledger --metadata status=processed

# Set multiple metadata
ledgerctl tx sm 42 -m reason="refund" -m ticket=JIRA-123

# Interactive mode (will prompt for metadata)
ledgerctl transactions set-metadata 42 --ledger my-ledger
```

#### transactions delete-metadata

Delete a metadata key from a transaction.

**Aliases:** `del-meta`, `dm`, `rm-meta`

```bash
ledgerctl transactions delete-metadata [transaction-id] [key] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Delete metadata key
ledgerctl transactions delete-metadata 42 status --ledger my-ledger

# Delete without confirmation
ledgerctl tx dm 42 reason -y

# Interactive mode
ledgerctl transactions delete-metadata
```

---

### store

![Operations Demo](../misc/demo/demo_operations.gif)

Storage operations.

**Aliases:** `s`

#### store metrics

Get metrics from the Pebble storage engine.

**Aliases:** `m`, `stats`

```bash
ledgerctl store metrics [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Display formatted metrics
ledgerctl store metrics

# Output as JSON
ledgerctl store metrics --json
```

#### store check

Verify store integrity by checking the hash chain and derived data consistency.

**Aliases:** `c`, `verify`

```bash
ledgerctl store check [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `50s` | Request timeout |

**Behavior:**
- Iterates all logs and verifies the BLAKE3 hash chain
- Replays logs to compute expected volumes and metadata
- Compares expected state against actual stored state
- Streams progress and errors in real-time

**Checks performed:**
- **SEQUENCE_GAP**: Missing log entries in the sequence
- **HASH_MISMATCH**: Log hash does not match expected hash chain value
- **VOLUME_MISMATCH**: Stored volume (input/output) does not match expected value from log replay
- **METADATA_MISMATCH**: Stored account metadata does not match expected value from log replay

**Example:**

```bash
# Check store integrity
ledgerctl store check

# Output as JSON (for scripting)
ledgerctl store check --json
```

#### store backup

Download a point-in-time backup of the Pebble store as a tar archive. The request is forwarded to the cluster leader to ensure the most up-to-date state.

**Aliases:** `bk`

```bash
ledgerctl store backup [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-o, --output` | | Output file path (required if stdout is a terminal) |
| `--timeout` | `100s` | Request timeout |

**Behavior:**
- Creates a fresh Pebble checkpoint on the leader node
- Streams the checkpoint as a tar archive
- Verifies SHA256 integrity on completion
- If connected to a follower, the request is automatically forwarded to the leader
- Refuses to write binary data to a terminal; use `--output` or pipe to a file

**Example:**

```bash
# Save backup to a file
ledgerctl store backup --output backup.tar

# Pipe to gzip
ledgerctl store backup | gzip > backup.tar.gz

# Short form
ledgerctl s bk -o backup.tar
```

---

### audit

View the replicated audit log. The audit log captures every proposal (success and failure) that goes through Raft consensus, providing a complete audit trail.

**Aliases:** `a`

#### audit list

List audit log entries via gRPC streaming.

**Aliases:** `ls`, `l`

```bash
ledgerctl audit list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--failures-only` | `false` | Show only failed entries |
| `--ledger` | | Filter by ledger name |
| `--after` | `0` | Show entries after this sequence number |
| `--limit` | `0` | Maximum number of entries to display (0 = unlimited) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Streams audit entries from the server
- Each entry shows: sequence number, timestamp, proposal ID, outcome (OK/FAIL), and ledger name
- If audit is disabled on the server, a warning message is displayed instead of an error

**Example:**

```bash
# List all audit entries
ledgerctl audit list

# Show only failures
ledgerctl audit list --failures-only

# Filter by ledger
ledgerctl audit list --ledger my-ledger

# Show entries after sequence 100
ledgerctl audit list --after 100

# Limit to 20 entries
ledgerctl audit list --limit 20

# Output as JSON
ledgerctl audit list --json
```

---

### cluster

Manage and inspect the Raft cluster.

**Aliases:** `cl`

#### cluster status

Display the current state of the Raft cluster, including node information and replication status.

**Aliases:** `st`

```bash
ledgerctl cluster status [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | `0` | Query specific node by ID (0 = route to leader) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- By default (`--node-id=0`), the request is automatically routed to the cluster leader
- Specify `--node-id` to query a specific node's state
- Displays cluster overview, Raft status, node list, and replication progress (if leader)

**Example:**

```bash
# Get cluster status from leader (default)
ledgerctl cluster status

# Get status from specific node (e.g., node 1)
ledgerctl cluster status --node-id 1

# Get status from node 2
ledgerctl cluster status --node-id 2
```

**Output sections:**
- **Cluster Overview**: State, local node ID, leader ID, total nodes
- **Raft Status**: Term, applied index, commit index, last index
- **Cluster Nodes**: List of all nodes with ID, address, suffrage, and status
- **Replication Progress**: Replication status for each follower (only shown when querying leader)

#### cluster transfer-leader

Transfer the Raft cluster leadership to a specific node. The request is automatically forwarded to the current leader.

**Aliases:** `tl`

```bash
ledgerctl cluster transfer-leader <node-id> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- The request is forwarded to the current leader if sent to a follower
- The leader synchronizes logs with the target node, then triggers an immediate election
- The command blocks until the new leader is confirmed or the timeout is reached

**Example:**

```bash
# Transfer leadership to node 2
ledgerctl cluster transfer-leader 2

# Transfer with custom timeout
ledgerctl cluster transfer-leader 3 --timeout 5s
```

#### cluster disk-usage

Display disk space used by storage components on the connected node.

**Aliases:** `du`

```bash
ledgerctl cluster disk-usage [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Returns disk usage from the node the CLI is directly connected to (no leader forwarding)
- Displays two sections: storage components (Spool, WAL, Data) and volumes (WAL, Data)

**Example:**

```bash
# Get disk usage from connected node
ledgerctl cluster disk-usage

# Output as JSON
ledgerctl cluster du --json
```

**Output sections:**
- **Storage Components**: Size of each storage component (Spool, WAL excluding spool, Data)
- **Volumes**: Used and total capacity of each storage volume (WAL including spool, Data)

---

## Connection Examples

### Local Development

```bash
# Connect to local server (default)
ledgerctl ledgers list

# Explicit local connection
ledgerctl --server localhost:8888 --insecure ledgers list
```

### Remote Server with TLS

```bash
# Connect to remote server with TLS (default)
ledgerctl --server ledger.example.com:443 ledgers list
```

### Remote Server without TLS

```bash
# Connect to remote server without TLS
ledgerctl --server ledger.example.com:8888 --insecure ledgers list
```

---

## Numscript Support

![Numscript Demo](../misc/demo/demo_numscript.gif)

The CLI supports creating transactions using Numscript files. All experimental Numscript features are **enabled by default**.

For complete documentation, see:
- [Numscript Guide](./numscript.md) - Complete guide with all features
- [Numscript Examples](../numscript/examples/README.md) - Ready-to-use scripts

### Enabled Features

| Feature | Description |
|---------|-------------|
| Account Interpolation | Dynamic addresses like `@escrow:$order_id` |
| Asset Colors | Track fund origins with colored assets |
| Get Amount/Asset | Extract components from monetary values |
| Mid-Script Calls | Query balances during execution |
| OneOf Selector | Conditional routing based on availability |
| Overdraft Function | Dynamic overdraft calculation |

### Variable Types

| Type | Format | Example |
|------|--------|---------|
| Account | `segment:segment:...` (without @) | `users:alice`, `bank` |
| Monetary | `ASSET/PRECISION AMOUNT` | `USD/2 100`, `EUR/2 5000` |
| String | Plain text | `order123` |

### Example Workflow

```bash
# 1. Create a ledger
ledgerctl ledgers create --name demo

# 2. Fund the bank from world
ledgerctl transactions create --ledger demo \
  --script numscript/examples/world_funding.num \
  --var "destination=bank" \
  --var "amount=USD/2 100000"

# 3. Transfer to a user
ledgerctl transactions create --ledger demo \
  --script numscript/examples/simple_transfer.num \
  --var "source=bank" \
  --var "destination=users:alice" \
  --var "amount=USD/2 1000"

# 4. Check balances
ledgerctl accounts get bank --ledger demo
ledgerctl accounts get users:alice --ledger demo
```

---

## Output Formats

### Table (default)

Human-readable tabular format, suitable for interactive use.

```bash
ledgerctl ledgers list
```

```
ID  NAME        CREATED AT
--  ----        ----------
1   my-ledger   2026-02-06T10:30:00Z
2   test        2026-02-06T11:00:00Z
```

### JSON

Machine-readable JSON format, suitable for scripting.

```bash
ledgerctl ledgers list --json
```

```json
{
  "my-ledger": {
    "id": 1,
    "name": "my-ledger",
    "createdAt": "2026-02-06T10:30:00Z"
  }
}
```

---

## Environment Variables

Global flags can be set via environment variables:

| Environment Variable | Flag |
|---------------------|------|
| `SERVER` | `--server` |
| `INSECURE` | `--insecure` |

```bash
export SERVER=ledger.example.com:443
export INSECURE=false
ledgerctl ledgers list
```
