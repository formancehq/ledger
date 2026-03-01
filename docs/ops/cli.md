# CLI Reference (ledgerctl)

`ledgerctl` is the command-line client for interacting with Ledger v3 servers via gRPC.

## Getting Started

![Getting Started](../../misc/demo/demo_getting_started.gif)

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
| `--tls-ca-cert` | | Path to CA certificate file (PEM) for server verification |
| `--signing-key` | | Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded) |
| `--signing-key-id` | `default` | Key ID for request signatures |
| `--response-verify-key` | | Path to Ed25519 public key file for verifying server response signatures |
| `--consistency` | | Read consistency level: `stale`, `leader`, or `linearizable` (default) |
| `--auth-token` | | Bearer token for authentication (JWT string or `@path-to-file`) |

### Read Consistency

By default, all read operations use **linearizable** consistency: the node performs a ReadIndex barrier to ensure it has applied all committed entries before reading from the local store. This guarantees that reads always reflect the latest committed state, but it can block during maintenance windows (e.g. mirror sync, snapshot creation) when the FSM is frozen.

Two alternative consistency levels are available:

- **`stale`** — Skip the ReadIndex barrier and read directly from the local store. Data may lag behind the latest committed index, but reads never block. Useful for monitoring, dashboards, and non-critical queries.
- **`leader`** — Forward the read to the leader node. The leader always has the most up-to-date data and its ReadIndex barrier is fast (no round-trip needed). Useful when you need fresh data but the local node may be lagging.

```bash
# Stale read (instant, may lag)
ledgerctl --consistency stale ledgers get my-ledger

# Leader read (fresh, forwarded to leader)
ledgerctl --consistency leader ledgers list

# Default linearizable read
ledgerctl ledgers list
```

### Request Signing

All write commands (create/delete ledger, create/revert transaction, set/delete metadata) support Ed25519 request signing. When `--signing-key` is provided, each request is signed before being sent to the server.

```bash
# Sign requests with a key file
ledgerctl --signing-key /path/to/seed.key ledgers create --name my-ledger

# Sign with a specific key ID
ledgerctl --signing-key /path/to/seed.key --signing-key-id admin-key-1 transactions create --ledger my-ledger --posting "world,bank,1000,USD"
```

The key file should contain a 32-byte Ed25519 seed, either as raw binary or hex-encoded text.

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

**Behavior:**
- Displays ledger name, creation timestamp, and mode (NORMAL or MIRROR)
- For mirror ledgers, displays the mirror source configuration (type, URL/DSN)
- For mirror ledgers, displays sync progress (state, cursor, source count, remaining, percentage)
- If the ledger has a metadata schema, it is displayed as tables (Account Fields, Transaction Fields) with KEY and TYPE columns

**Example:**

```bash
# Get a normal ledger
ledgerctl ledgers get my-ledger

# Get a mirror ledger (shows sync progress)
ledgerctl ledgers get my-mirror-ledger
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
| `--schema` | | Metadata schema in `target:key:type` format (repeatable) |
| `--mode` | `normal` | Ledger mode: `normal` or `mirror` |
| `--mirror-source-type` | `http` | Mirror source type: `http` or `postgres` |
| `--mirror-ledger-name` | | Source ledger name in the v2 system (defaults to ledger name) |
| `--mirror-base-url` | | Base URL of the v2 API (required for `http` source) |
| `--mirror-auth-token` | | Auth token for the v2 API (for `http` source) |
| `--mirror-dsn` | | PostgreSQL DSN (required for `postgres` source) |
| `--mirror-batch-size` | `0` | Max logs per batch (0 = server default, capped by `--mirror-max-batch-size`) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Schema format:** `target:key:type` where target is `account` or `transaction`, and type is one of: `string`, `int64`, `bool`, `uint64`, `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`.

**Behavior:**
- In interactive mode (no `--schema` flags), prompts "Add metadata schema?" and loops through target/key/type selection
- If the ledger is created with a schema, the schema is displayed in the output
- For mirror mode, displays the mirror source configuration in the output

**Example:**

```bash
# Create a normal ledger
ledgerctl ledgers create --name my-ledger

# Create with metadata
ledgerctl ledgers create --name my-ledger --metadata description="My ledger" --metadata env=prod

# Create with typed metadata schema
ledgerctl ledgers create --name my-ledger --schema account:age:int64 --schema account:active:bool

# Create a mirror ledger from an HTTP v2 source
ledgerctl ledgers create --name my-mirror \
  --mode mirror \
  --mirror-base-url https://v2-api.example.com \
  --mirror-auth-token my-token

# Create a mirror ledger from a PostgreSQL v2 source
ledgerctl ledgers create --name my-mirror \
  --mode mirror \
  --mirror-source-type postgres \
  --mirror-dsn "postgres://user:pass@host:5432/ledger?sslmode=disable"

# Mirror with a different source ledger name
ledgerctl ledgers create --name my-mirror \
  --mode mirror \
  --mirror-base-url https://v2-api.example.com \
  --mirror-ledger-name original-ledger-name

# Interactive mode (will prompt for name, metadata schema)
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

#### ledgers promote

Promote a mirror ledger to normal mode. This stops mirror replication and converts the ledger to a regular read-write ledger.

```bash
ledgerctl ledgers promote [name] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | | Name of the ledger to promote |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Promote a mirror ledger (will prompt for confirmation)
ledgerctl ledgers promote my-mirror-ledger

# Promote without confirmation
ledgerctl ledgers promote my-mirror-ledger -y

# Interactive mode (will prompt for ledger selection)
ledgerctl ledgers promote
```

#### ledgers set-metadata-type

Declare a typed metadata field on a ledger. Once set, all new metadata values for this key must conform to the declared type. Existing untyped values will be converted in the background.

**Aliases:** `set-type`, `smt`

```bash
ledgerctl ledgers set-metadata-type [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--target` | | Target type: `account` or `transaction` |
| `--key` | | Metadata key name |
| `--type` | | Metadata type: `string`, `int64`, `bool`, `uint64`, `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32` |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- If `--ledger` is not provided and only one ledger exists, it will be used automatically
- If multiple ledgers exist, you will be prompted to select one
- Missing flags will be prompted interactively

**Example:**

```bash
# Set a field type with all flags
ledgerctl ledgers set-metadata-type --ledger my-ledger --target account --key age --type int64

# Set a transaction field type
ledgerctl ledgers smt --ledger my-ledger --target transaction --key priority --type uint64

# Interactive mode (will prompt for all inputs)
ledgerctl ledgers set-metadata-type
```

#### ledgers remove-metadata-type

Remove a typed metadata field declaration from a ledger. After removal, the key will accept values of any type again.

**Aliases:** `rm-type`, `rmt`

```bash
ledgerctl ledgers remove-metadata-type [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--target` | | Target type: `account` or `transaction` |
| `--key` | | Metadata key name to remove |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- If `--ledger` is not provided and only one ledger exists, it will be used automatically
- If multiple ledgers exist, you will be prompted to select one
- Prompts for confirmation before removing (use `-y` to skip)
- Missing flags will be prompted interactively

**Example:**

```bash
# Remove a field type
ledgerctl ledgers remove-metadata-type --ledger my-ledger --target account --key age

# Remove without confirmation
ledgerctl ledgers rmt --ledger my-ledger --target account --key age -y

# Interactive mode
ledgerctl ledgers remove-metadata-type
```

#### ledgers get-schema

Display the metadata schema for a ledger including conversion status. Shows two tables (Account Fields, Transaction Fields) with KEY, TYPE, and STATUS columns.

**Aliases:** `schema`, `gs`

```bash
ledgerctl ledgers get-schema <name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Shows account and transaction field types with their conversion status (COMPLETE or CONVERTING)
- If no schema is defined, displays "(no schema defined)"

**Example:**

```bash
# Get schema status
ledgerctl ledgers get-schema my-ledger

# Output as JSON
ledgerctl ledgers get-schema my-ledger --json

# Using alias
ledgerctl ledgers schema my-ledger
```

---

### accounts

![Metadata Demo](../../misc/demo/demo_metadata.gif)

Manage accounts in a ledger.

**Aliases:** `account`, `acc`, `a`

#### accounts list

List accounts in a ledger with pagination.

**Aliases:** `ls`, `l`

```bash
ledgerctl accounts list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--page-size` | `10` | Number of accounts per page |
| `--prefix` | | Filter accounts by address prefix (e.g. `users:`) |
| `--filter` | | Filter expression (see [Filter Expression Syntax](#filter-expression-syntax)) |
| `--all` | `false` | Fetch all accounts at once (no pagination) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Accounts are listed in alphabetical order
- If `--ledger` is not provided and only one ledger exists, it will be used automatically
- If multiple ledgers exist, you will be prompted to select one
- Use `--prefix` to filter by address prefix (e.g. `users:` lists only accounts starting with `users:`)
- Use `--filter` for rich boolean filter expressions on metadata and addresses
- If both `--prefix` and `--filter` are provided, they are combined with AND

**Example:**

```bash
# List accounts with explicit ledger
ledgerctl accounts list --ledger my-ledger

# Filter by prefix
ledgerctl accounts list --ledger my-ledger --prefix users:

# Filter by metadata
ledgerctl accounts list --ledger my-ledger --filter "metadata[category] == premium"

# Complex filter expression
ledgerctl accounts list --ledger my-ledger --filter "metadata[active] == true and address ^= users:"

# Combine prefix and filter (equivalent to AND)
ledgerctl accounts list --ledger my-ledger --prefix users: --filter "metadata[tier] == gold"

# Fetch all accounts at once
ledgerctl accounts list --ledger my-ledger --all

# Output as JSON
ledgerctl accounts list --ledger my-ledger --json
```

##### Filter Expression Syntax

The `--filter` flag accepts a human-readable boolean expression that maps to the underlying `QueryFilter` model.

**Grammar:**

```
expression     := or_expr
or_expr        := and_expr ("or" and_expr)*
and_expr       := unary_expr ("and" unary_expr)*
unary_expr     := "not" unary_expr | primary
primary        := "(" expression ")" | condition
condition      := metadata_cond | address_cond
metadata_cond  := "metadata" "[" KEY "]" ("==" VALUE | "!=" VALUE | "exists")
address_cond   := "address" ("==" VALUE | "^=" VALUE)
```

**Conditions:**

| Syntax | Description |
|--------|-------------|
| `metadata[key] == value` | Metadata equality (auto-typed: `true`/`false` → bool, integer → int64, else → string) |
| `metadata[key] != value` | Metadata inequality (desugars to `not (metadata[key] == value)`) |
| `metadata[key] exists` | Metadata key existence check |
| `address == value` | Exact address match |
| `address ^= value` | Address prefix match |

**Boolean operators** (precedence: `not` > `and` > `or`):

| Operator | Description |
|----------|-------------|
| `and` | Both conditions must match |
| `or` | At least one condition must match |
| `not` | Negation |
| `(expr)` | Grouping to override precedence |

**Values:**

| Format | Example | Type |
|--------|---------|------|
| Bare word | `premium` | string |
| Quoted string | `"hello world"` or `'hello world'` | string |
| Integer | `42`, `-5` | int64 |
| Boolean | `true`, `false` | bool |

**Examples:**

```bash
# Simple metadata match
--filter "metadata[category] == premium"

# Quoted value with spaces
--filter 'metadata[name] == "John Doe"'

# Boolean metadata
--filter "metadata[active] == true"

# Integer metadata
--filter "metadata[age] == 42"

# Metadata existence
--filter "metadata[category] exists"

# Address prefix
--filter "address ^= users:"

# Exact address
--filter 'address == "users:alice"'

# AND (both must match)
--filter "metadata[active] == true and address ^= users:"

# OR (either must match)
--filter "metadata[category] == premium or metadata[category] == gold"

# NOT
--filter "not metadata[blocked] == true"

# Grouping
--filter "(metadata[a] == x or metadata[b] == y) and address ^= users:"
```

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

#### accounts analyze

Analyze all accounts in a ledger and suggest a Chart of Accounts based on discovered address patterns. Useful after a mirror import (v2 to v3) to understand account structure.

**Aliases:** `analyse`

```bash
ledgerctl accounts analyze [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--threshold` | `0` | Variable threshold (0 = default 10): max distinct children before classifying as variable |
| `--json` | `false` | Output full response as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Scans all accounts and builds a trie from colon-separated address segments
- Classifies segments as fixed (constant labels) or variable (IDs, numbers)
- Infers regex patterns for variable segments (UUID, numeric, alphanumeric)
- Outputs a suggested Chart of Accounts tree, discovered patterns, and statistics
- If `--ledger` is not provided and only one ledger exists, it will be used automatically

**Example:**

```bash
# Analyze accounts with rich terminal output
ledgerctl accounts analyze --ledger my-ledger

# Increase variable threshold for ledgers with many fixed sub-accounts
ledgerctl accounts analyze --ledger my-ledger --threshold 20

# Output as JSON (for programmatic consumption)
ledgerctl accounts analyze --ledger my-ledger --json
```

---

### transactions

![Transactions Demo](../../misc/demo/demo_transactions.gif)

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

Get detailed information about a transaction. If the server has receipt signing configured, the response includes a JWT receipt.

**Aliases:** `g`, `show`, `describe`

```bash
ledgerctl transactions get [transaction-id] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--json` | `false` | Output as JSON (includes receipt) |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Get transaction by ID
ledgerctl transactions get 42 --ledger my-ledger

# Interactive mode
ledgerctl transactions get

# Get transaction with receipt (displayed if signing key is configured)
ledgerctl transactions get 42 --ledger my-ledger --json
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
| `--expand-volumes` | `false` | Include post-commit volumes (per account/asset) in response |
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
| `--receipt` | | JWT receipt for the transaction (avoids server-side lookup) |
| `--expand-volumes` | `false` | Include post-commit volumes (per account/asset) in response |
| `-y, --yes` | `false` | Skip confirmation prompt |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Creates a new transaction that reverses all postings from the original
- By default, prompts for confirmation before reverting
- Use `-y` or `--yes` to skip the confirmation prompt
- Use `--force` to revert even if funds have already been spent from receiving accounts
- Use `--receipt` to provide a JWT receipt (obtained from `transactions get` or the original create response); the server will extract the postings from the receipt instead of reading from storage

**Example:**

```bash
# Revert a transaction (will prompt for confirmation)
ledgerctl transactions revert 42 --ledger my-ledger

# Force revert even if funds have been spent
ledgerctl transactions revert 42 --ledger my-ledger --force

# Revert at the original transaction timestamp
ledgerctl transactions revert 42 --ledger my-ledger --at-effective-date

# Revert using a receipt (avoids server-side transaction lookup)
ledgerctl transactions revert 42 --ledger my-ledger --receipt <jwt-token>

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

![Operations Demo](../../misc/demo/demo_operations.gif)

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

### store bootstrap

Build a data directory from a backup tar file without starting a server. This is a purely offline operation useful for scripted disaster recovery or bootstrapping from backups.

```bash
ledgerctl store bootstrap --input backup.tar --data-dir /path/to/data [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-i, --input` | | Path to the backup tar file (required) |
| `--data-dir` | | Target data directory (required, must be fresh) |
| `--validate` | `false` | Run integrity checks after extraction |
| `-y, --yes` | `false` | Skip confirmation prompt |

**Behavior:**

1. Verifies the target data directory is fresh (no `CURRENT_CHECKPOINT` file)
2. Extracts the tar archive into a staging directory
3. Opens the staging as a read-only Pebble database and displays a preview (ledger count, timestamps)
4. If `--validate` is set, runs the full integrity checker (same as `store check`)
5. Prompts for confirmation (unless `--yes`)
6. Hard-links staging to `checkpoints/0`, writes `CURRENT_CHECKPOINT` and `RESTORED` marker
7. Cleans up the staging directory

After bootstrap, start the server with `--bootstrap` to use the restored data.

**Example:**

```bash
# Interactive with validation
ledgerctl store bootstrap --input backup.tar --data-dir ./fresh-data --validate

# Non-interactive (scripted)
ledgerctl store bootstrap -i backup.tar --data-dir ./fresh-data --yes
```

---

### store rebuild-indexes

Rebuild the bbolt read indexes from Pebble system logs. This is a purely offline operation — no server needed. Use this after restoring from a backup or when the read index becomes corrupted or out of date.

```bash
ledgerctl store rebuild-indexes --data-dir /path/to/data [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | | Pebble data directory (required) |
| `--read-index-dir` | | Read index output directory (default: `<data-dir>/read-indexes/`) |

**Behavior:**

1. Opens the Pebble data directory in read-only mode
2. Opens or creates the bbolt read index store
3. Replays all system logs from scratch, rebuilding inverted indexes for metadata, account/transaction existence, and account-to-transaction mappings
4. Reports the last processed log sequence on completion

**Example:**

```bash
# Rebuild with default read index location
ledgerctl store rebuild-indexes --data-dir ./data

# Rebuild to a custom read index directory
ledgerctl store rebuild-indexes --data-dir ./data --read-index-dir ./custom-indexes
```

---

### audit

View the replicated audit log. The audit log captures every proposal (success and failure) that goes through Raft consensus, providing a complete audit trail.

**Aliases:** `a`

#### audit enable

Enable audit logging on the server. When enabled, all Raft proposals are recorded in the audit log.

```bash
ledgerctl audit enable [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |
| `--signing-key` | | Path to Ed25519 seed file for request signing |

**Example:**

```bash
# Enable audit logging
ledgerctl audit enable

# Enable with signed request
ledgerctl audit enable --signing-key /path/to/seed
```

#### audit disable

Disable audit logging on the server. When disabled, proposals are no longer recorded in the audit log.

```bash
ledgerctl audit disable [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |
| `--signing-key` | | Path to Ed25519 seed file for request signing |

**Example:**

```bash
# Disable audit logging
ledgerctl audit disable

# Disable with signed request
ledgerctl audit disable --signing-key /path/to/seed
```

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
| `--page-size` | `10` | Number of entries per page (0 = unlimited) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Streams audit entries from the server
- Each entry shows: sequence number, timestamp, proposal ID, and outcome (OK/FAIL)
- Below each entry, all orders are listed in a tree structure with:
  - Order type and details (ledger name, reference, etc.)
  - Signing key ID used (`key=<id>`) or `unsigned` if the order was not signed
  - Consecutive identical orders are grouped compactly (e.g. `MirrorIngest x500`)
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

# Show 20 entries per page
ledgerctl audit list --page-size 20

# Output as JSON
ledgerctl audit list --json
```

**Sample output:**

```
  #1      2025-01-01T00:00:00Z  proposal=1    OK  logs=[1]
    └─ CreateLedger name=default  key=unsigned
  #2      2025-01-01T00:00:01Z  proposal=2    OK  logs=[2]
    └─ CreateTransaction ledger=default ref=tx-001  key=admin-key
  #3      2025-01-01T00:00:02Z  proposal=3    FAIL  [INSUFFICIENT_FUNDS] ...
    └─ CreateTransaction ledger=default  key=admin-key
```

#### audit get

Get a single audit entry by its sequence number.

```bash
ledgerctl audit get <sequence> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Retrieves a single audit entry by sequence number
- Displays the same tree format as `audit list` for the single entry
- Returns an error if the entry does not exist

**Example:**

```bash
# Get audit entry #5
ledgerctl audit get 5

# Get as JSON
ledgerctl audit get 5 --json
```

---

### logs

View system logs. System logs record every state change (ledger creation/deletion, transactions, metadata, signing keys, etc.) in the global log.

**Aliases:** `log`

#### logs list

List system log entries via gRPC streaming.

**Aliases:** `ls`, `l`

```bash
ledgerctl logs list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--after` | `0` | Show logs after this sequence number |
| `--page-size` | `10` | Number of logs per page (0 = unlimited) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Streams system log entries from the server
- Each entry shows: sequence number, log type, ledger name (if applicable), and details

**Example:**

```bash
# List all system logs
ledgerctl logs list

# Show logs after sequence 100
ledgerctl logs list --after 100

# Show 20 entries per page
ledgerctl logs list --page-size 20

# Output as JSON
ledgerctl logs list --json
```

#### logs get

Get a single system log entry by sequence number.

```bash
ledgerctl logs get <sequence> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Get log #42
ledgerctl logs get 42

# Get log as JSON
ledgerctl logs get 42 --json
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

#### cluster watch

Continuously poll and display the cluster status with in-place updates (similar to the Unix `watch` command).

```bash
ledgerctl cluster watch [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `2s` | Polling interval |
| `--node-id` | `0` | Query specific node by ID (0 = route to leader) |
| `--timeout` | `10s` | Per-request timeout |

**Behavior:**
- Polls the cluster state at the configured interval and updates the display in place
- No banner is shown (saves screen space); a refresh timestamp is displayed at the bottom
- On error (e.g., unreachable server), the error is displayed in place and polling continues
- Press Ctrl+C to exit cleanly

**Example:**

```bash
# Watch cluster status with default 2s interval
ledgerctl cluster watch

# Watch with 1s interval
ledgerctl cluster watch --interval 1s

# Watch a specific node
ledgerctl cluster watch --node-id 2

# Watch with custom timeout
ledgerctl cluster watch --interval 5s --timeout 3s
```

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

#### cluster add-learner

Add a non-voting (learner) node to the Raft cluster. The request is forwarded to the current leader.

```bash
ledgerctl cluster add-learner <node-id> <raft-address> <service-address> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- The request is forwarded to the current leader if sent to a follower
- The leader proposes a ConfChange to add the node as a learner (non-voting member)
- Once committed, all nodes add the learner to their transport and service pool
- The learner receives log entries and snapshots but cannot vote or become leader

**Example:**

```bash
# Add node 4 as a learner
ledgerctl cluster add-learner 4 node-4:7777 node-4:8888

# Add a learner using custom timeout
ledgerctl cluster add-learner 5 node-5:7777 node-5:8888 --timeout 30s
```

#### cluster promote-learner

Promote a learner (non-voting) node to a full voter in the Raft cluster. The request is forwarded to the leader.

```bash
ledgerctl cluster promote-learner <node-id> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `30s` | Request timeout |

**Behavior:**
- The request is forwarded to the current leader if sent to a follower
- The leader proposes a ConfChange to promote the learner to a voter
- Once committed, the node participates in elections and can become leader

**Example:**

```bash
# Promote learner node 4 to voter
ledgerctl cluster promote-learner 4

# Promote with custom timeout
ledgerctl cluster promote-learner 5 --timeout 60s
```

#### cluster remove-node

Remove a node (voter or learner) from the Raft cluster. The request is forwarded to the leader. Cannot remove the leader itself; transfer leadership first.

```bash
ledgerctl cluster remove-node <node-id> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |
| `--force` | `false` | Bypass Raft consensus (unsafe, for permanently unreachable nodes) |

**Behavior:**
- The request is forwarded to the current leader if sent to a follower
- The leader proposes a ConfChange to remove the node from the cluster
- Once committed, all nodes remove the peer from their transport and service pool
- Cannot remove the leader node; use `cluster transfer-leader` first
- Works for both voters and learners
- The removed node is not automatically shut down; the operator must stop it manually

**Force mode (`--force`):**
- Bypasses Raft consensus by directly applying the configuration change on the leader
- Must be executed on the leader node (not forwarded)
- Use only for permanently unreachable nodes where consensus-based removal would block
- Useful when a downed node causes quorum loss (e.g., 3→1 scale-down with a crashed node)
- The change is persisted to the WAL snapshot and survives leader restarts

**Example:**

```bash
# Remove node 3 from the cluster
ledgerctl cluster remove-node 3

# Remove with custom timeout
ledgerctl cluster remove-node 4 --timeout 30s

# Force-remove a crashed node (bypasses consensus)
ledgerctl cluster remove-node 3 --force
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

#### cluster maintenance

Enable or disable cluster maintenance mode. When enabled, all write operations (Raft commands) are blocked at the admission layer. Only the maintenance mode command itself is allowed through (to disable maintenance mode). Read operations continue to work normally.

```bash
ledgerctl cluster maintenance <true|false|enable|disable> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Blocks all write operations when enabled (transactions, metadata changes, ledger creation/deletion, etc.)
- Allows the `SetMaintenanceMode` request itself to pass through (to disable maintenance mode)
- Read operations (list ledgers, get accounts, etc.) continue to work normally
- The maintenance mode flag is replicated through Raft consensus
- Visible in `cluster status` output

**Example:**

```bash
# Enable maintenance mode
ledgerctl cluster maintenance enable

# Disable maintenance mode
ledgerctl cluster maintenance disable

# With request signing
ledgerctl cluster maintenance enable --signing-key /path/to/seed
```

### auth

Ed25519 authentication utilities for key generation and token creation. See [Authentication Guide](authentication.md) for full details.

#### auth generate-key

Generate an Ed25519 keypair for JWT authentication.

```bash
ledgerctl auth generate-key <output-directory>
```

Creates `seed.hex` (private, mode 0600) and `pubkey.hex` (public) in the output directory.

#### auth generate-token

Generate a signed EdDSA JWT token for use with servers configured with `--auth-ed25519-keys`.

```bash
ledgerctl auth generate-token \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot \
  --scopes ledger:read,ledger:write \
  --expiration 1h
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--signing-key` | yes | | Path to Ed25519 seed file |
| `--key-id` | yes | | Key ID matching the server config |
| `--subject` | yes | | JWT subject claim |
| `--scopes` | no | | Comma-separated scopes |
| `--expiration` | no | `1h` | Token validity duration |

The token is printed to stdout and can be used with `--auth-token` or the `Authorization: Bearer` header.

### signing

![Signing Demo](../../misc/demo/demo_signing.gif)

Manage Ed25519 signing keys and signature configuration.

Signing keys are managed dynamically via the gRPC API (not server-side config files).
The first key registration can be unsigned (bootstrap). Once keys exist, all key management
operations must be signed by an existing key.

**Aliases:** `sign`, `keys`

#### signing list-keys

List all registered signing keys and their parent relationships.

**Aliases:** `ls`, `list`

```bash
ledgerctl signing list-keys
```

**Output columns:**

| Column | Description |
|--------|-------------|
| Key ID | Unique identifier of the key |
| Public Key (hex) | Ed25519 public key (hex-encoded) |
| Parent | Parent key ID, or `(root)` for bootstrap keys |

**Example:**

```bash
# List all signing keys
ledgerctl signing list-keys

# With remote server
ledgerctl --server node1:8888 signing list-keys
```

#### signing generate-key

Generate an Ed25519 keypair for request signing. Creates two hex-encoded files in the specified output directory.

**Aliases:** `gen-key`, `keygen`

```bash
ledgerctl signing generate-key <output-directory>
```

**Output files:**
- `seed.hex` — 32-byte Ed25519 seed (hex-encoded, mode 0600), used with `--signing-key`
- `pubkey.hex` — 32-byte Ed25519 public key (hex-encoded), used with `signing register-key`

**Example:**

```bash
# Generate a keypair
ledgerctl signing generate-key ./my-keys

# Use the generated files
ledgerctl signing register-key --key-id admin --public-key-file ./my-keys/pubkey.hex
ledgerctl --signing-key ./my-keys/seed.hex ledgers create --name my-ledger
```

#### signing register-key

Register an Ed25519 public key for signature verification.

**Aliases:** `add-key`, `register`

```bash
# Bootstrap: register the first key (unsigned)
ledgerctl signing register-key --key-id admin --public-key-file /path/to/pubkey.hex

# Register with hex-encoded public key
ledgerctl signing register-key --key-id admin --public-key <hex-encoded-32-bytes>

# Register additional key (must be signed by existing key)
ledgerctl signing register-key --key-id ops --public-key-file /path/to/pubkey.hex --signing-key /path/to/seed
```

| Flag | Required | Description |
|------|----------|-------------|
| `--key-id` | Yes | Unique identifier for the key |
| `--public-key` | One of | Ed25519 public key as hex-encoded string (32 bytes) |
| `--public-key-file` | One of | Path to file containing Ed25519 public key (raw 32 bytes or hex-encoded) |
| `--timeout` | No | Request timeout (default: 10s) |

#### signing revoke-key

Revoke a registered signing key. Must be signed by an existing key.

**Aliases:** `remove-key`, `revoke`

```bash
ledgerctl signing revoke-key --key-id ops --signing-key /path/to/seed
```

| Flag | Required | Description |
|------|----------|-------------|
| `--key-id` | Yes | Key ID to revoke |
| `--cascade` | No | Also revoke all descendant keys (default: false) |
| `--timeout` | No | Request timeout (default: 10s) |

#### signing require

Enable or disable mandatory request signatures. Must be signed by an existing key.

```bash
# Enable mandatory signatures
ledgerctl signing require true --signing-key /path/to/seed

# Disable mandatory signatures
ledgerctl signing require false --signing-key /path/to/seed
```

| Argument | Description |
|----------|-------------|
| `true` / `false` | Enable or disable mandatory signatures (also accepts `1`/`0`, `yes`/`no`, `on`/`off`, `enable`/`disable`) |

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

---

### periods

Manage accounting periods.

**Aliases:** `period`, `pd`

#### periods list

List all periods with their status.

```bash
ledgerctl periods list
```

**Output columns:**

| Column | Description |
|--------|-------------|
| ID | Period identifier |
| Status | OPEN, CLOSING, CLOSED, or ARCHIVED |
| Start | Period start timestamp |
| End | Period end timestamp (set when closed) |
| Close Seq | Log sequence at which the period was closed |

**Example:**

```bash
# List all periods
ledgerctl periods list

# With remote server
ledgerctl --server node1:8888 periods list
```

#### periods close

Close the current open period and open a new one. A background seal process will compute the sealing hash.

```bash
ledgerctl periods close
```

**Example:**

```bash
# Close the current period
ledgerctl periods close

# Output:
#  SUCCESS  Period 1 closed successfully
#  INFO  New period 2 opened
#  INFO  Background sealing process will compute the sealing hash
```

#### periods set-schedule

Set a cron schedule for automatic period rotation. The schedule is stored in Raft and takes effect immediately on the leader.

```bash
ledgerctl periods set-schedule <cron-expression>
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

The cron expression uses the standard 5-field format (`minute hour day-of-month month day-of-week`) or the extended 6-field format with an optional leading seconds field (`second minute hour day-of-month month day-of-week`).

**Examples:**

```bash
# Rotate every day at midnight
ledgerctl periods set-schedule "0 0 * * *"

# Rotate on the 1st of every month at midnight
ledgerctl periods set-schedule "0 0 1 * *"

# Rotate every 30 seconds (6-field format)
ledgerctl periods set-schedule "*/30 * * * * *"
```

#### periods delete-schedule

Remove the cron schedule for automatic period rotation, disabling automatic rotation.

```bash
ledgerctl periods delete-schedule
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

**Example:**

```bash
ledgerctl periods delete-schedule
#  SUCCESS  Period schedule deleted
```

#### periods get-schedule

Display the current cron schedule for automatic period rotation.

```bash
ledgerctl periods get-schedule
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

**Example:**

```bash
ledgerctl periods get-schedule

# Output (schedule set):
#  SUCCESS  Period schedule: 0 0 1 * *

# Output (no schedule):
#  SUCCESS  No period schedule configured (automatic rotation disabled)
```

#### periods archive

Archive a closed period to cold storage. This exports logs and audit entries to the configured cold storage backend and purges them from hot storage. Attributes (volumes, metadata) remain in Pebble.

```bash
ledgerctl periods archive <period-id>
```

**Example:**

```bash
# Archive period 1 (must be in CLOSED state)
ledgerctl periods archive 1

# Output:
#  SUCCESS  Period 1 archival initiated
#  INFO  Background archiver will export data to cold storage and confirm
```

**Notes:**
- The period must be in `CLOSED` state (sealed). `OPEN`, `CLOSING`, or `ARCHIVED` periods are rejected.
- Archival is asynchronous: the command returns immediately after validation, and a background Archiver exports the data and confirms the transition to `ARCHIVED`.
- Cold storage is configured on the server with `--cold-storage-driver`, `--cold-storage-path`, and S3 flags (`--cold-storage-bucket-id`, `--cold-storage-s3-bucket`, `--cold-storage-s3-region`, `--cold-storage-s3-endpoint`).

---

### restore

Backup restore operations. Requires the server to be started with `--restore`.

#### restore upload

Upload a backup tar archive to the restore staging area.

```bash
ledgerctl restore upload --input backup.tar
```

| Flag | Required | Description |
|------|----------|-------------|
| `--input`, `-i` | Yes | Input tar file path |
| `--timeout` | No | Request timeout (default: 100s) |

#### restore validate

Run integrity checks on the staged backup data (hash chain, volumes, metadata).

```bash
ledgerctl restore validate
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 50s) |

#### restore preview

Display a summary of the staged backup data.

```bash
ledgerctl restore preview
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

#### restore finalize

Commit the staged backup as live data and shut down the server.

```bash
# With confirmation prompt
ledgerctl restore finalize

# Skip confirmation
ledgerctl restore finalize --yes
```

| Flag | Required | Description |
|------|----------|-------------|
| `--yes`, `-y` | No | Skip confirmation prompt |
| `--timeout` | No | Request timeout (default: 10s) |

---

### Server `--restore` Flag

Start the server in restore mode:

```bash
ledger-v3-poc run --node-id 1 --data-dir ./data --restore --grpc-port 8888
```

In restore mode:
- Only the RestoreService gRPC endpoint and `/health` HTTP endpoint are available
- No Raft, WAL, or other production services are started
- Requires a fresh data directory (no `CURRENT_CHECKPOINT`)

After finalizing, restart without `--restore`:

```bash
ledger-v3-poc run --node-id 1 --data-dir ./data --bootstrap --wal-dir ./wal --grpc-port 8888
```

### Server `--numscript-cache-size` Flag

Controls the maximum number of parsed Numscript programs kept in an LRU cache. When the cache is full, the least recently used entry is evicted.

| Flag | Default | Description |
|------|---------|-------------|
| `--numscript-cache-size` | `1024` | Maximum number of parsed Numscript programs to cache (LRU eviction) |

```bash
# Use default (1024 entries)
ledger-v3-poc run --node-id 1 --bootstrap ...

# Increase cache for workloads with many distinct scripts
ledger-v3-poc run --node-id 1 --bootstrap --numscript-cache-size 4096 ...
```

### Server `--mirror-max-batch-size` Flag

Server-side cap on the mirror batch size. Each mirror ledger can request a custom batch size via its source config, but the server clamps it to this maximum. This prevents a user from overwhelming the cluster with oversized batches.

| Flag | Default | Description |
|------|---------|-------------|
| `--mirror-max-batch-size` | `500` | Maximum allowed batch size for mirror sync |

```bash
# Default: mirror workers use at most 500 logs per batch
ledger-v3-poc run --node-id 1 --bootstrap ...

# Allow larger batches for high-throughput mirror workloads
ledger-v3-poc run --node-id 1 --bootstrap --mirror-max-batch-size 1000 ...
```

---

### Server `--unsafe-skip-config-validation` Flag

Skips the startup configuration safety checks that prevent accidental changes to critical parameters (`node-id`, `cluster-id`) between restarts with existing data.

| Flag | Default | Description |
|------|---------|-------------|
| `--unsafe-skip-config-validation` | `false` | Skip startup configuration safety checks (DANGEROUS) |

On first boot, the server persists `node-id` and `cluster-id` into Pebble. On subsequent boots, the server compares these values against the current flags and refuses to start if they differ. This prevents silent data corruption from accidentally pointing a node at the wrong data directory or changing identity.

**When to use this flag:**
- After intentionally changing `node-id` or `cluster-id` (e.g., migrating data between clusters)
- Never in normal operation

```bash
# Normal operation: server refuses to start if node-id changed
ledger-v3-poc run --node-id 2 --data-dir ./data  # ERROR if data was created with --node-id 1

# Override with explicit flag
ledger-v3-poc run --node-id 2 --data-dir ./data --unsafe-skip-config-validation
```

---

### Server `--response-signing-key` Flag

Enables Ed25519 response signing. When configured, the server signs every `Log` in `ApplyResponse` messages so clients can verify the response is authentic.

| Flag | Default | Description |
|------|---------|-------------|
| `--response-signing-key` | | Path to Ed25519 seed file for response signing (empty = disabled) |

The seed file must contain 32 bytes (raw binary) or 64 hex characters. Generate one with `ledgerctl signing generate-key`.

```bash
# Generate a keypair for response signing
ledgerctl signing generate-key ./response-keys

# Start server with response signing
ledger-v3-poc run --response-signing-key ./response-keys/seed.hex [other flags...]

# Client-side: verify response signatures
ledgerctl --response-verify-key ./response-keys/pubkey.hex transactions create --ledger my-ledger --posting "world,bank,1000,USD"
```

Clients can also discover the server's public key via the `Discovery` RPC.

---

### Server Read Index Flags

The bbolt-based read index store is always active. An index builder tails the system logs and populates inverted indexes in a bbolt database. The read index is used for prepared queries and listing operations (accounts, transactions).

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--read-index-dir` | string | `""` | Directory for the bbolt read index database (default: `<data-dir>/read-indexes/`) |

```bash
# Use default directory (<data-dir>/read-indexes/)
ledger-v3-poc run [other flags...]

# Use custom directory
ledger-v3-poc run --read-index-dir /ssd/read-indexes [other flags...]
```

The index builder runs on ALL nodes (not just the leader), so follower nodes can also serve prepared query reads. Listings are eventually consistent (the bbolt index may lag behind the latest Raft commits).

After restoring from a backup, use `ledgerctl store rebuild-indexes` to backfill the index from existing data.

---

### Server Authentication Flags

Enable JWT/OIDC authentication with scope-based authorization. See [Authentication Guide](authentication.md) for full details.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-enabled` | bool | `false` | Enable JWT authentication |
| `--auth-issuer` | string | `""` | OIDC issuer URL (used for discovery and token validation) |
| `--auth-check-scopes` | bool | `false` | Enforce scope-based authorization |
| `--auth-service` | string | `""` | Service name prefix for scopes (e.g., `ledger` for `ledger:read`) |
| `--auth-read-key-set-max-retries` | int | `10` | Maximum retries when fetching the JWKS key set |
| `--auth-ed25519-keys` | string | `""` | Path to JSON file with Ed25519 public keys and scopes (auto-enables auth) |

```bash
# Start server with OIDC authentication
ledger-v3-poc run \
  --auth-enabled \
  --auth-issuer https://auth.example.com \
  --auth-check-scopes \
  --auth-service ledger \
  [other flags...]

# Start server with Ed25519 key-based authentication
ledger-v3-poc run \
  --auth-ed25519-keys auth-keys.json \
  [other flags...]
```

When enabled, the server performs OIDC discovery, downloads the JWKS, and validates JWT signatures, issuer, and expiration on every request. Three scopes are used: `ledger:read`, `ledger:write`, `ledger:admin`.

When `--auth-ed25519-keys` is set, both OIDC and Ed25519 authentication can coexist. See [Authentication Guide](authentication.md) for full Ed25519 setup instructions.

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
# Connect to remote server with TLS (default, uses system CA pool)
ledgerctl --server ledger.example.com:443 ledgers list

# Connect with a custom CA certificate (e.g., self-signed or internal CA)
ledgerctl --server ledger.example.com:8888 --tls-ca-cert /path/to/ca.pem ledgers list
```

### Remote Server without TLS

```bash
# Connect to remote server without TLS
ledgerctl --server ledger.example.com:8888 --insecure ledgers list
```

---

## Numscript Support

![Numscript Demo](../../misc/demo/demo_numscript.gif)

The CLI supports creating transactions using Numscript files. All experimental Numscript features are **enabled by default**.

For complete documentation, see:
- [Numscript Guide](../dev/numscript.md) - Complete guide with all features
- [Numscript Examples](../../misc/numscript/examples/README.md) - Ready-to-use scripts

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
| `TLS_CA_CERT` | `--tls-ca-cert` |
| `CONSISTENCY` | `--consistency` |

```bash
export SERVER=ledger.example.com:443
export INSECURE=false
export TLS_CA_CERT=/path/to/ca.pem
ledgerctl ledgers list
```

---

## Event Sinks

Manage event sinks (NATS, ClickHouse, Kafka, HTTP) that receive domain events derived from the global log.

### `events list`

List all configured event sinks and their current status (cursor position, errors).

```bash
ledgerctl events list
```

**Aliases:** `ls`, `sinks`

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

### `events add-sink`

Add or update (upsert) a named event sink configuration. The configuration is replicated via Raft consensus.

Currently supported sink types: **NATS JetStream**, **ClickHouse**, **Kafka**, **HTTP**.

```bash
# Add a NATS sink with default settings
ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events

# Add a NATS sink with custom batch settings and protobuf format
ledgerctl events add-sink --name primary --nats-url nats://localhost:4222 --nats-topic ledger.events \
  --format protobuf --batch-size 128 --batch-delay-ms 50

# Add a ClickHouse sink for analytics
ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db

# Add a ClickHouse sink with custom table name
ledgerctl events add-sink --name analytics --ch-dsn clickhouse://user:pass@localhost:9000/db --ch-table my_events

# Add a Kafka sink
ledgerctl events add-sink --name streaming --kafka-brokers localhost:9092 --kafka-topic ledger-events

# Add a Kafka sink with SASL authentication
ledgerctl events add-sink --name streaming --kafka-brokers broker1:9092,broker2:9092 --kafka-topic ledger-events \
  --kafka-tls --kafka-sasl-mechanism SCRAM-SHA-256 --kafka-sasl-username user --kafka-sasl-password pass

# Add an HTTP webhook sink
ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger

# Add an HTTP webhook sink with HMAC signature verification
ledgerctl events add-sink --name webhook --http-endpoint https://example.com/webhooks/ledger --http-secret my-secret
```

**Aliases:** `add`, `upsert`

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | *(required)* | Unique name for this sink |
| `--nats-url` | | NATS server URL (required for NATS sinks) |
| `--nats-topic` | | NATS topic/subject for events (required for NATS sinks) |
| `--ch-dsn` | | ClickHouse DSN (required for ClickHouse sinks, e.g. `clickhouse://user:pass@host:9000/db`) |
| `--ch-table` | `ledger_events` | ClickHouse table name |
| `--kafka-brokers` | | Kafka broker addresses (comma-separated, required for Kafka sinks) |
| `--kafka-topic` | | Kafka topic name (required for Kafka sinks) |
| `--kafka-tls` | `false` | Enable TLS for Kafka connection |
| `--kafka-sasl-mechanism` | | Kafka SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) |
| `--kafka-sasl-username` | | Kafka SASL username |
| `--kafka-sasl-password` | | Kafka SASL password |
| `--http-endpoint` | | HTTP webhook endpoint URL (required for HTTP sinks) |
| `--http-secret` | | HMAC-SHA256 secret for `X-Webhook-Signature` header |
| `--format` | `json` | Event serialization format (`json` or `protobuf`) |
| `--batch-size` | `64` | Max events per batch |
| `--batch-delay-ms` | `10` | Max delay before flush in ms |
| `--timeout` | `10s` | Request timeout |

You must specify exactly one sink type: NATS (`--nats-url` + `--nats-topic`), ClickHouse (`--ch-dsn`), Kafka (`--kafka-brokers` + `--kafka-topic`), or HTTP (`--http-endpoint`).

The HTTP sink sends each event as an individual POST request with headers:
- `Content-Type`: `application/json` or `application/protobuf`
- `X-Event-Type`: event type (e.g. `committed_transaction`)
- `X-Ledger`: ledger name
- `X-Log-Sequence`: global log sequence number
- `X-Webhook-Signature`: `sha256=<hex>` HMAC signature (only when `--http-secret` is set)

### `events remove-sink`

Remove a named event sink. If this is the last sink, event emission is implicitly disabled.

```bash
ledgerctl events remove-sink --name primary
```

**Aliases:** `rm`, `delete-sink`

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | *(required)* | Name of the sink to remove |
| `--timeout` | `10s` | Request timeout |

See [Event System Architecture](../dev/architecture/events.md) for details on the event system design.
