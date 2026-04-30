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
| `--profile` | | Connection profile name (env: `LEDGERCTL_PROFILE`) |
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
| `--mirror-oauth2-client-id` | | OAuth2 client ID for the v2 API (for `http` source) |
| `--mirror-oauth2-client-secret` | | OAuth2 client secret for the v2 API (for `http` source) |
| `--mirror-oauth2-token-endpoint` | | OAuth2 token endpoint URL (for `http` source) |
| `--mirror-oauth2-scopes` | | OAuth2 scopes (for `http` source, repeatable) |
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

# Create a mirror ledger from an HTTP v2 source with OAuth2
ledgerctl ledgers create --name my-mirror \
  --mode mirror \
  --mirror-base-url https://v2-api.example.com \
  --mirror-oauth2-client-id my-client-id \
  --mirror-oauth2-client-secret my-client-secret \
  --mirror-oauth2-token-endpoint https://auth.example.com/token

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

#### ledgers stats

Get aggregate statistics (account count, transaction count) for a ledger.

**Aliases:** `st`

```bash
ledgerctl ledgers stats [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Ledger name (interactive selection if omitted) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Get stats for a specific ledger
ledgerctl ledgers stats --ledger my-ledger

# Get stats as JSON
ledgerctl ledgers stats --ledger my-ledger --json

# Interactive mode (will prompt for ledger selection)
ledgerctl ledgers stats
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

#### ledgers create-index

Create an opt-in index on a ledger. Indexes are built in the background and queries will be rejected until the index reaches READY status.

**Aliases:** `ci`

```bash
ledgerctl ledgers create-index [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--type` | | Index type: `address`, `source-address`, `dest-address`, `metadata`, `reference`, `timestamp`, `inserted-at` |
| `--target` | | Target type for metadata index: `account` or `transaction` |
| `--key` | | Metadata key name (for metadata index) |
| `--timeout` | `10s` | Request timeout |

**Index types:**

| Type | Description |
|------|-------------|
| `address` | Account-to-transaction mapping for any posting role |
| `source-address` | Source account-to-transaction mapping |
| `dest-address` | Destination account-to-transaction mapping |
| `metadata` | Metadata field index (requires `--target` and `--key`) |
| `reference` | Transaction reference exact-match index |
| `timestamp` | Transaction timestamp (effective date) range-scan index |
| `inserted-at` | Transaction creation date (`inserted_at`) range-scan index |

> **Note:** Filtering by transaction ID (`id`) does not require any index — it is always available via a direct range scan.

**Behavior:**
- The index starts building in the background immediately
- Queries using the index will be rejected until the index reaches READY status
- Creating an index that already exists and is READY is idempotent (no error)

**Example:**

```bash
# Create address index (any role)
ledgerctl ledgers create-index --ledger my-ledger --type address

# Create source-only address index
ledgerctl ledgers create-index --ledger my-ledger --type source-address

# Create metadata index
ledgerctl ledgers create-index --ledger my-ledger --type metadata --target account --key category

# Create reference index (enables filtering transactions by reference)
ledgerctl ledgers create-index --ledger my-ledger --type reference

# Create timestamp index (enables filtering transactions by effective date range)
ledgerctl ledgers create-index --ledger my-ledger --type timestamp

# Create inserted-at index (enables filtering transactions by creation date range)
ledgerctl ledgers create-index --ledger my-ledger --type inserted-at

# Interactive mode
ledgerctl ledgers create-index
```

#### ledgers drop-index

Drop an opt-in index from a ledger. This stops the index from being updated.

**Aliases:** `di`

```bash
ledgerctl ledgers drop-index [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--type` | | Index type: `address`, `source-address`, `dest-address`, `metadata`, `reference`, `timestamp`, `inserted-at` |
| `--target` | | Target type for metadata index: `account` or `transaction` |
| `--key` | | Metadata key name (for metadata index) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Queries using the dropped index will be rejected after the drop
- Dropping a non-existent index returns an error

**Example:**

```bash
# Drop address index
ledgerctl ledgers drop-index --ledger my-ledger --type address

# Drop metadata index
ledgerctl ledgers drop-index --ledger my-ledger --type metadata --target account --key category

# Drop reference index
ledgerctl ledgers drop-index --ledger my-ledger --type reference
```

#### ledgers list-indexes

List all configured indexes on a ledger with their build status.

**Aliases:** `li`, `indexes`

```bash
ledgerctl ledgers list-indexes [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Shows all active indexes with their build status (BUILDING or READY)
- If no indexes are configured, shows a hint to create one

**Example:**

```bash
# List all indexes
ledgerctl ledgers list-indexes --ledger my-ledger
```

**Sample output:**

```
TYPE             TARGET       KEY        STATUS
address          -            -          READY
source-address   -            -          BUILDING (42%)
metadata         account      category   READY
reference        -            -          READY
timestamp        -            -          BUILDING (starting...)
```

#### ledgers catalog

Show a ledger's full configuration catalog: chart of accounts, indexes, prepared queries, and numscript library.

**Aliases:** `cat`

```bash
ledgerctl ledgers catalog <name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--expand` | `false` | Show full content of numscripts and prepared query filters |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Summary view (default)
ledgerctl ledgers catalog myledger

# Expanded view with numscript source code and query filters
ledgerctl ledgers catalog myledger --expand
```

**Sample output:**

```
Catalog for ledger: myledger
═════════════════════════════════════

 Chart of Accounts (STRICT)

  bank
    main  (account)
  users
    :id [a-z0-9]+  (account)
      wallets
        :currency [A-Z]{3}  (account)

 Indexes

TYPE             TARGET       KEY        STATUS
address          -            -          READY
metadata         account      category   READY

 Prepared Queries

NAME           TARGET
active-users   accounts

 Numscript Library

NAME       VERSION   CREATED AT
transfer   2.0.0     2025-01-15T10:30:00Z
refund     1.0.0     2025-01-15T10:30:00Z
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
| `--reverse` | `false` | Reverse iteration order (Z→A instead of A→Z) |
| `--all` | `false` | Fetch all accounts at once (no pagination) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Accounts are listed in alphabetical order by default; use `--reverse` for reverse-alphabetical (Z→A)
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
condition      := metadata_cond | address_cond | source_cond | destination_cond
metadata_cond  := "metadata" "[" KEY "]" ("==" VALUE | "!=" VALUE | ">" VALUE | ">=" VALUE | "<" VALUE | "<=" VALUE | "exists")
address_cond   := "address" ("==" VALUE | "^=" VALUE)
source_cond    := "source" ("==" VALUE | "^=" VALUE)
destination_cond := "destination" ("==" VALUE | "^=" VALUE)
```

**Conditions:**

| Syntax | Description |
|--------|-------------|
| `metadata[key] == value` | Metadata equality (auto-typed: `true`/`false` → bool, integer → int64, else → string) |
| `metadata[key] != value` | Metadata inequality (desugars to `not (metadata[key] == value)`) |
| `metadata[key] > value` | Metadata greater than (integer values only) |
| `metadata[key] >= value` | Metadata greater than or equal (integer values only) |
| `metadata[key] < value` | Metadata less than (integer values only) |
| `metadata[key] <= value` | Metadata less than or equal (integer values only) |
| `metadata[key] exists` | Metadata key existence check |
| `address == value` | Exact address match (any role: source or destination) |
| `address ^= value` | Address prefix match (any role: source or destination) |
| `source == value` | Exact source address match (transactions only) |
| `source ^= value` | Source address prefix match (transactions only) |
| `destination == value` | Exact destination address match (transactions only) |
| `destination ^= value` | Destination address prefix match (transactions only) |

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

**Schema validation:** When a ledger declares a metadata schema (e.g., `account:age:int64`), filter conditions are validated against the declared types at compile time. Type mismatches produce clear error messages (e.g., using a string condition on an `int64` field). Integer conditions on unsigned fields (e.g., `uint64`) are automatically coerced to unsigned conditions. The `exists` condition is always valid regardless of schema type.

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

# Integer range (greater than)
--filter "metadata[age] > 18"

# Integer range (combined with AND)
--filter "metadata[score] >= 50 and metadata[score] <= 100"

# Metadata existence
--filter "metadata[category] exists"

# Address prefix
--filter "address ^= users:"

# Exact address
--filter 'address == "users:alice"'

# Source address (transactions only)
--filter 'source ^= "merchants:"'

# Destination address (transactions only)
--filter 'destination == "users:alice"'

# Source AND destination combined
--filter 'source ^= "merchants:" and destination ^= "users:"'

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
| `--filter` | | Filter expression (e.g. `"metadata[category] == premium"`) |
| `--reverse` | `false` | Reverse iteration order (oldest first instead of newest first) |
| `--all` | `false` | Fetch all transactions at once (no pagination) |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Transactions are displayed **newest first** by default; use `--reverse` for oldest first
- Interactive pagination: press Enter to load the next page, or 'q' to quit
- In JSON mode, only the first page is output (no interactive pagination)

**Example:**

```bash
# List transactions with interactive pagination
ledgerctl transactions list --ledger my-ledger

# Custom page size
ledgerctl transactions list --ledger my-ledger --page-size 20

# Oldest first
ledgerctl transactions list --ledger my-ledger --reverse

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

#### transactions analyze

Analyze all transactions in a ledger and discover flow patterns by normalizing account addresses. Shows statistics per flow type including temporal distribution and volume metrics.

**Aliases:** `analyse`

```bash
ledgerctl transactions analyze [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Name of the ledger |
| `--threshold` | `0` | Variable threshold (0 = default 10): max distinct children before classifying as variable |
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Analyze transactions with rich terminal output
ledgerctl transactions analyze --ledger my-ledger

# Increase variable threshold
ledgerctl transactions analyze --ledger my-ledger --threshold 20

# Output as JSON (for programmatic consumption)
ledgerctl transactions analyze --ledger my-ledger --json
```

---

### account-types

Manage account types (pattern-based account validation).

**Aliases:** `at`, `types`

#### account-types add

Add a new account type to a ledger.

```bash
ledgerctl account-types add <name> <pattern> --ledger <ledger> [--persistence <mode>]
```

**Arguments:**
- `name` — Unique name for the account type (e.g., `user-checking`)
- `pattern` — Address pattern with optional variables (e.g., `users:{id}:checking`)

**Flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | *(required)* | Target ledger name |
| `--persistence` | `normal` | Volume persistence mode (see below) |

**Persistence modes:**
| Mode | Description |
|------|-------------|
| `normal` | Default. Volumes are fully persisted. |
| `ephemeral` | Volumes are persisted but purged when the account reaches zero balance (input == output). |
| `transient` | Volumes are never written to storage. The account must have zero balance at the end of each batch. Ideal for staging/intermediary accounts that are only used within a single batch. |

**Pattern syntax:**
- Fixed segments: `users`, `bank`, `checking`
- Variable segments: `{id}`, `{name}`
- Regex-constrained: `{iban:^[A-Z]{2}[0-9]{14}$}`

**Example:**
```bash
ledgerctl at add user-checking "users:{id}:checking" --ledger my-ledger
ledgerctl at add bank-main "banks:{iban:^[A-Z]{2}[0-9]{14}$}:main" --ledger my-ledger
ledgerctl at add fx-clearing "fx:clearing" --ledger my-ledger --persistence ephemeral
ledgerctl at add staging "staging:{txhash}" --ledger my-ledger --persistence transient
```

#### account-types list

List all account types for a ledger.

```bash
ledgerctl account-types list [--ledger <ledger>]
```

If `--ledger` is not provided and only one ledger exists, it will be used automatically.

**Example:**
```bash
ledgerctl at ls --ledger my-ledger
```

#### account-types get

Get details of a specific account type.

```bash
ledgerctl account-types get <name> [--ledger <ledger>]
```

Shows name, pattern, status, and persistence mode.

#### account-types remove

Remove an account type from a ledger.

```bash
ledgerctl account-types remove <name> --ledger <ledger>
```

**Aliases:** `rm`, `delete`

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

#### store read-index-metrics

Get metrics from the read index Pebble store.

```bash
ledgerctl store read-index-metrics [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Display formatted metrics
ledgerctl store read-index-metrics

# Output as JSON
ledgerctl store read-index-metrics --json
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

#### store compact

Trigger a synchronous compaction of the local Pebble store. Useful after bulk deletes, period archival, or before taking a backup.

**Aliases:** `gc`

```bash
ledgerctl store compact [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `50s` | Request timeout |

**Behavior:**
- Runs a prefix-by-prefix compaction on the connected node (node-local, not forwarded to leader)
- Blocks until all prefixes are compacted
- Returns the wall-clock duration of the compaction

**Example:**

```bash
# Compact the local store
ledgerctl store compact

# Output as JSON (for scripting)
ledgerctl store compact --json

# Short form
ledgerctl s gc
```

#### store checkpoint

Create a Pebble checkpoint of the current live database state. Useful after `store compact` to persist the compacted state so it survives restarts (which restore from the latest checkpoint).

**Aliases:** `cp`

```bash
ledgerctl store checkpoint [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Creates a new Pebble checkpoint from the current `live/` directory (node-local, not forwarded to leader)
- Updates `CURRENT_CHECKPOINT` so the next restart uses this checkpoint
- Returns the new checkpoint ID

**Example:**

```bash
# Create a checkpoint after compaction
ledgerctl store compact && ledgerctl store checkpoint

# Output as JSON (for scripting)
ledgerctl store checkpoint --json

# Short form
ledgerctl s cp
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

Rebuild the Pebble read indexes from system logs. This is a purely offline operation — no server needed. Use this after restoring from a backup or when the read index becomes corrupted or out of date.

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
2. Opens or creates the Pebble read index database
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
| `--after` | `0` | Show logs after this global sequence number |
| `--page-size` | `10` | Number of logs per page (0 = unlimited) |
| `--min-log-sequence` | `0` | Minimum log sequence the server must have applied before reading (0 = no constraint) |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Streams system log entries from the server
- Each entry shows: sequence number, log type, ledger name (if applicable), and details
- The underlying gRPC `ListLogs` RPC supports a `QueryFilter` for per-ledger listing (e.g. `ledger == "foo"`) and log ID pagination (`log_id > 42`). The per-ledger log index must be enabled via `CreateIndex` (builtin `LOG` index)

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

Authentication utilities for key generation, token creation, and credential storage. See [Authentication Guide](authentication.md) for full details.

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
| `--store` | no | `false` | Store the generated token in the OS keychain (keyed by `--server`) |

The token is printed to stdout and can be used with `--auth-token` or the `Authorization: Bearer` header. When `--store` is set, the token is also stored in the OS keychain for the current `--server` address, and a confirmation is printed to stderr.

```bash
# Generate and store in keychain
ledgerctl auth generate-token \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot \
  --store

# Generate, store, and also pipe to a file
ledgerctl auth generate-token \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot \
  --store > token.txt
```

#### auth login

Generate a signed EdDSA JWT token and store it in the OS keychain for the current `--server` address. Subsequent commands automatically use the stored token without `--auth-token`.

```bash
ledgerctl auth login [flags]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--signing-key` | * | | Path to Ed25519 seed file |
| `--key-id` | * | | Key ID matching the server config |
| `--subject` | * | | JWT subject claim |
| `--scopes` | no | | Comma-separated scopes |
| `--expiration` | no | `1h` | Token validity duration |
| `--bundle` | no | | Path to JSON key bundle file (or `-` for stdin) |

\* Required when not using `--bundle` or stdin pipe. When a bundle is provided, explicit flags override bundle values.

**Behavior:**
- Generates a signed JWT token using the provided Ed25519 key (from flags or bundle)
- Stores the token in the OS keychain (macOS Keychain, Linux libsecret, Windows Credential Manager)
- Displays a JWT summary (subject, expiry, scopes)
- Accepts a JSON key bundle via `--bundle <path>`, `--bundle -`, or a piped stdin

**Bundle format:**

```json
{
  "signingKey": "<64-char hex seed>",
  "keyId": "agent-key-id",
  "scopes": ["ledger:read", "ledger:write"],
  "subject": "my-agent"
}
```

**Example:**

```bash
# Login with flags (generate + store)
ledgerctl auth login \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot \
  --scopes ledger:read,ledger:write

# Login with a bundle file
ledgerctl auth login --bundle agent-bundle.json

# Login from a piped bundle (e.g. from kubectl-ledger)
kubectl ledger agents get-key my-agent --bundle - | ledgerctl auth login

# Override the subject from a bundle
kubectl ledger agents get-key my-agent --bundle - | ledgerctl auth login --subject ci-bot

# All subsequent commands use the stored token automatically
ledgerctl ledgers list

# Login to a different server
ledgerctl --server prod:8888 auth login \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot
```

#### auth logout

Remove the stored token from the OS keychain for the current `--server` address.

```bash
ledgerctl auth logout
```

**Behavior:**
- Removes the token for the current `--server` from the OS keychain
- Gracefully handles the case when no token is stored

**Example:**

```bash
# Remove token for default server
ledgerctl auth logout

# Remove token for a specific server
ledgerctl --server prod:8888 auth logout
```

#### auth status

Show the current authentication status and decoded JWT claims.

**Aliases:** `whoami`

```bash
ledgerctl auth status
```

**Behavior:**
- Shows the token source (flag, environment, keychain, or none)
- Decodes JWT claims without signature verification
- Displays: server, source, subject, key ID, scopes, issued/expiry, valid/expired status

**Example:**

```bash
# Check auth status for default server
ledgerctl auth status

# Check for a specific server
ledgerctl --server prod:8888 auth status

# Using alias
ledgerctl auth whoami
```

### provision

Run pre-built provisioning scenarios against a cluster. Scenarios create ledgers, account types, numscripts, and sample transactions to bootstrap a realistic environment.

#### provision list

List all available provisioning scenarios.

**Aliases:** `ls`

```bash
ledgerctl provision list
```

#### provision run

Run a named provisioning scenario against the connected cluster.

```bash
ledgerctl provision run <scenario-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `120s` | Request timeout (longer default for provisioning) |

**Available scenarios:**

| Name | Description |
|------|-------------|
| `gaming-wallet` | Gaming platform with virtual currency, 20 players, coin purchases, promotions, P2P trades |
| `lending-lifecycle` | Consumer lending with 10 loans, 6-month repayment, defaults, write-offs |
| `marketplace` | E-commerce marketplace with 50 customers, 10 merchants, 200 purchases with fees |
| `multi-currency` | Corporate treasury with FX operations across USD, EUR, GBP |
| `multi-ledger-payroll` | Multi-ledger payroll with 3 departments, clearing ledger, cost allocations |
| `subscription` | SaaS billing with 50 subscribers, 3 monthly cycles, revenue recognition |

**Example:**

```bash
# List available scenarios
ledgerctl provision list

# Run a scenario
ledgerctl provision run gaming-wallet --server localhost:8888 --insecure

# Run with custom timeout
ledgerctl provision run marketplace --timeout 300s
```

### profile

Manage named connection profiles. Each profile stores a server address and TLS settings. Auth tokens in the OS keychain are keyed by server address, so switching profiles automatically switches auth context.

**Aliases:** `profiles`, `prof`

**Config file location:** `~/.config/ledgerctl/config.json` (Linux), `~/Library/Application Support/ledgerctl/config.json` (macOS)

**Flag resolution priority:** Explicit CLI flag > Environment variable > Profile value > Cobra default

#### profile create

Create a new connection profile.

```bash
ledgerctl profile create <name> --server <addr> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | *(required)* | gRPC server address |
| `--insecure` | `false` | Use insecure connection (no TLS) |
| `--tls-ca-cert` | | Path to CA certificate file (PEM) |
| `--use` | `false` | Set this profile as the active profile |

**Behavior:**
- Errors if the profile name already exists
- Automatically sets the profile as active if it's the first profile
- Use `--use` to activate immediately even when other profiles exist

**Example:**

```bash
# Create a local development profile
ledgerctl profile create local --server localhost:8888 --insecure

# Create a production profile and activate it
ledgerctl profile create prod --server ledger.prod.example.com:443 --use

# Create a profile with custom CA
ledgerctl profile create staging --server ledger.staging.example.com:443 --tls-ca-cert /path/to/ca.pem
```

#### profile list

List all connection profiles.

**Aliases:** `ls`, `l`

```bash
ledgerctl profile list
```

**Behavior:**
- Shows a table with all profiles sorted by name
- Active profile is marked with `*`
- Shows a hint if no profiles are configured

**Example:**

```bash
ledgerctl profile list
#    NAME     SERVER                          INSECURE  TLS CA CERT
# *  prod     ledger.prod.example.com:443
#    local    localhost:8888                  true
#    staging  ledger.staging.example.com:443            /path/to/ca.pem
```

#### profile use

Set the active connection profile.

```bash
ledgerctl profile use <name>
```

**Behavior:**
- Errors if the profile name is not found

**Example:**

```bash
# Switch to production profile
ledgerctl profile use prod

# Switch to local development
ledgerctl profile use local
```

#### profile delete

Delete a connection profile.

**Aliases:** `rm`, `remove`

```bash
ledgerctl profile delete <name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-y, --yes` | `false` | Skip confirmation prompt |

**Behavior:**
- Prompts for confirmation before deleting (use `-y` to skip)
- Clears the active profile if the deleted profile was active

**Example:**

```bash
# Delete with confirmation prompt
ledgerctl profile delete staging

# Delete without confirmation
ledgerctl profile delete staging -y
```

#### profile show

Show details of a connection profile.

**Aliases:** `get`, `describe`

```bash
ledgerctl profile show [name]
```

**Behavior:**
- Defaults to the active profile if no name is given
- Shows server address, TLS settings, and auth status (whether a token is stored in the keychain for this profile's server)

**Example:**

```bash
# Show active profile
ledgerctl profile show

# Show a specific profile
ledgerctl profile show prod
```

---

**Profile workflow example:**

```bash
# Create profiles for different environments
ledgerctl profile create local --server localhost:8888 --insecure
ledgerctl profile create prod --server ledger.prod.example.com:443

# Switch to production
ledgerctl profile use prod

# Auth works naturally with profiles (token keyed by server address)
ledgerctl auth login --bundle key.json
ledgerctl auth status

# Override profile for a single command
ledgerctl --profile local ledgers list

# Environment variable override
LEDGERCTL_PROFILE=local ledgerctl ledgers list

# Explicit flags always win over profile values
ledgerctl --server other:8888 ledgers list
```

---

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

### numscripts

Manage the numscript library (per-ledger reusable scripts with semver versioning).

**Aliases:** `ns`

**Persistent Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Ledger name (interactive selection if omitted) |

#### numscripts list

List all numscripts in a ledger's library (latest version of each).

**Aliases:** `ls`

```bash
ledgerctl numscripts list --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

#### numscripts get

Get a numscript from the library by name.

```bash
ledgerctl numscripts get <name> --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--version` | | Specific version to retrieve (empty = latest) |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Get the latest version
ledgerctl numscripts get transfer --ledger myledger

# Get a specific version
ledgerctl numscripts get transfer --ledger myledger --version 1.0.0
```

#### numscripts save

Save a numscript to a ledger's library. If a script with the same name already exists, a new version is created.

```bash
ledgerctl numscripts save <name> --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | | Path to the numscript file (reads stdin if omitted) |
| `--version` | | Semver version (e.g. `1.0.0`) or empty for latest |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
# Save from a file
ledgerctl numscripts save transfer --ledger myledger --file transfer.num

# Save with a specific version
ledgerctl numscripts save transfer --ledger myledger --file transfer.num --version 2.0.0

# Save from stdin
cat transfer.num | ledgerctl numscripts save transfer --ledger myledger
```

#### numscripts delete

Delete a numscript from the library.

**Aliases:** `rm`, `remove`

```bash
ledgerctl numscripts delete <name> --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

---

### queries

Manage prepared queries — named, parameterized query templates stored in the primary store.

**Aliases:** `query`, `pq`

#### queries create

Create a named prepared query for a ledger.

```bash
ledgerctl queries create <name> --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Ledger name (interactive selection if omitted) |
| `--target` | `accounts` | Query target: `accounts`, `transactions`, or `logs` |
| `--filter` | | Filter expression (same DSL as account/transaction list) |
| `--timeout` | `10s` | Request timeout |

**Examples:**

```bash
# Query accounts with specific metadata
ledgerctl queries create active-users --ledger my-ledger --target accounts --filter "metadata[active] == true"

# Query transactions above a threshold
ledgerctl queries create big-txns --ledger my-ledger --target transactions --filter "amount > 1000"

# Parameterized query (parameters prefixed with $)
ledgerctl queries create by-tier --ledger my-ledger --target accounts --filter "metadata[tier] == \$tier"
```

#### queries list

List all prepared queries for a ledger.

**Aliases:** `ls`, `l`

```bash
ledgerctl queries list --ledger <ledger-name> [flags]
```

Displays each query's name, target, and filter in human-readable DSL format.

#### queries update

Update the filter of an existing prepared query.

```bash
ledgerctl queries update <name> --ledger <ledger-name> --filter "<new-filter>" [flags]
```

#### queries delete

Delete a prepared query.

**Aliases:** `rm`

```bash
ledgerctl queries delete <name> --ledger <ledger-name> [flags]
```

#### queries execute

Execute a prepared query and display results.

**Aliases:** `exec`, `run`

```bash
ledgerctl queries execute <name> --ledger <ledger-name> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--ledger` | | Ledger name (interactive selection if omitted) |
| `--param` | | Query parameter as `key=value` (repeatable) |
| `--page-size` | `10` | Number of results per page |
| `--mode` | `list` | Query mode: `list` or `aggregate` |
| `--min-log-sequence` | `0` | Minimum log sequence before reading |
| `--analyze` | `false` | Display query execution profile |
| `--timeout` | `10s` | Request timeout |

**Examples:**

```bash
# Execute without parameters
ledgerctl queries execute active-users --ledger my-ledger

# Execute with parameters
ledgerctl queries execute by-tier --ledger my-ledger --param tier=gold

# Aggregate mode (returns per-asset volumes)
ledgerctl queries execute active-users --ledger my-ledger --mode aggregate

# Multiple parameters
ledgerctl queries execute filtered --ledger my-ledger --param status=active --param region=eu
```

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

### Server `--sentinel-mode` Flag

Enables sentinel mode: runtime volume consistency assertions that verify correctness at every Raft apply. See [Sentinel Mode](./sentinel-mode.md) for full details.

| Flag | Default | Description |
|------|---------|-------------|
| `--sentinel-mode` | `false` | Enable runtime volume consistency assertions (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification) |

When enabled, four checks run in the write path: volume monotonicity, delta/posting cross-check, aggregated volume balance, and post-commit cache/Pebble verification. Intended for testing and staging environments.

```bash
# Enable sentinel mode
ledger-v3-poc run --sentinel-mode [other flags...]

# Via environment variable
SENTINEL_MODE=true ledger-v3-poc run [other flags...]
```

---

### Server Bloom Filter Flags

Application-level bloom filters that avoid Pebble reads for keys known not to exist. Each attribute type has its own filter with independent sizing. Set `expected-keys` to `0` to disable a type.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--bloom-volumes-expected-keys` | uint | `100000000` | Expected unique volume keys (0 = disable) |
| `--bloom-volumes-fp-rate` | float64 | `0.01` | False positive rate for volumes (0.01 = 1%) |
| `--bloom-metadata-expected-keys` | uint | `10000000` | Expected unique metadata keys (0 = disable) |
| `--bloom-metadata-fp-rate` | float64 | `0.01` | False positive rate for metadata |
| `--bloom-idempotency-expected-keys` | uint | `10000000` | Expected unique idempotency keys (0 = disable) |
| `--bloom-idempotency-fp-rate` | float64 | `0.01` | False positive rate for idempotency |
| `--bloom-references-expected-keys` | uint | `10000000` | Expected unique reference keys (0 = disable) |
| `--bloom-references-fp-rate` | float64 | `0.01` | False positive rate for references |
| `--bloom-ledgers-expected-keys` | uint | `0` | Expected unique ledger keys (0 = disabled by default) |
| `--bloom-ledgers-fp-rate` | float64 | `0.01` | False positive rate for ledgers |
| `--bloom-boundaries-expected-keys` | uint | `0` | Expected unique boundary keys (0 = disabled by default) |
| `--bloom-boundaries-fp-rate` | float64 | `0.01` | False positive rate for boundaries |
| `--bloom-transactions-expected-keys` | uint | `0` | Expected unique transaction keys (0 = disabled by default) |
| `--bloom-transactions-fp-rate` | float64 | `0.01` | False positive rate for transactions |

Ledger, boundary, and transaction filters are disabled by default because these attribute types are rarely read and don't benefit from bloom filtering.

```bash
# Default config (volumes, metadata, idempotency, references enabled)
ledger-v3-poc run [other flags...]

# Disable all bloom filters
ledger-v3-poc run --bloom-volumes-expected-keys 0 --bloom-metadata-expected-keys 0 \
  --bloom-idempotency-expected-keys 0 --bloom-references-expected-keys 0

# Enable transaction filter for a specific workload
ledger-v3-poc run --bloom-transactions-expected-keys 50000000
```

Changing any bloom filter configuration triggers a full repopulation scan on next startup.

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

The Pebble-based read index store is always active. An index builder tails the system logs and populates inverted indexes in a separate Pebble database. The read index is used for prepared queries and listing operations (accounts, transactions). WAL is disabled because the index is a derived view that can be rebuilt from Raft logs.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--read-index-dir` | string | `""` | Directory for the Pebble read index database (default: `<data-dir>/read-indexes/`) |
| `--read-index-batch-size` | int | `1000` | Log entries per write batch. Larger batches reduce flush frequency but use more memory per batch. |
| `--read-index-memtable-size` | uint64 | `67108864` (64 MB) | Read index memtable size in bytes |
| `--read-index-memtable-stop-writes-threshold` | int | `4` | Read index memtable count before stopping writes |
| `--read-index-cache-size` | int64 | `67108864` (64 MB) | Read index block cache size in bytes |
| `--read-index-l0-compaction-threshold` | int | `4` | Read index L0 file count to trigger compaction |
| `--read-index-l0-stop-writes-threshold` | int | `12` | Read index L0 file count before stopping writes |
| `--read-index-lbase-max-bytes` | int64 | `536870912` (512 MB) | Read index L1 max size in bytes |
| `--read-index-target-file-size` | int64 | `67108864` (64 MB) | Read index SST file target size in bytes |
| `--read-index-bytes-per-sync` | int | `524288` (512 KB) | Read index bytes written before sync |
| `--read-index-max-concurrent-compactions` | int | `1` | Read index max concurrent compactions |

```bash
# Use default directory (<data-dir>/read-indexes/)
ledger-v3-poc run [other flags...]

# Use custom directory
ledger-v3-poc run --read-index-dir /ssd/read-indexes [other flags...]

# Increase read index cache for better read performance
ledger-v3-poc run --read-index-cache-size 134217728 [other flags...]
```

The index builder runs on ALL nodes (not just the leader), so follower nodes can also serve prepared query reads. Listings are eventually consistent (the read index may lag behind the latest Raft commits).

After restoring from a backup, use `ledgerctl store rebuild-indexes` to backfill the index from existing data.

---

### Server Pebble Storage Flags

Tune the Pebble (LSM-tree) storage engine. All sizes are in bytes unless specified.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--pebble-memtable-size` | uint64 | `268435456` (256 MB) | Size of a single memtable |
| `--pebble-memtable-stop-writes-threshold` | int | `6` | Number of memtables before stopping writes |
| `--pebble-l0-compaction-threshold` | int | `4` | L0 file count to trigger compaction |
| `--pebble-l0-stop-writes-threshold` | int | `16` | L0 file count before stopping writes |
| `--pebble-lbase-max-bytes` | int64 | `2147483648` (2 GB) | Maximum size of L1 |
| `--pebble-cache-size` | int64 | `1073741824` (1 GB) | Block cache size |
| `--pebble-target-file-size` | int64 | `268435456` (256 MB) | Target SST file size |
| `--pebble-bytes-per-sync` | int | `1048576` (1 MB) | Bytes written before sync during flush/compaction |
| `--pebble-wal-bytes-per-sync` | int | `1048576` (1 MB) | WAL bytes written before sync |
| `--pebble-max-concurrent-compactions` | int | `2` | Maximum concurrent compactions |
| `--pebble-wal-min-sync-interval` | duration | `0` | Minimum interval between WAL syncs (0 = immediate) |
| `--pebble-disable-wal` | bool | `false` | Disable WAL entirely (WARNING: risks data loss) |

---

### Server Authentication Flags

Enable JWT/OIDC authentication with scope-based authorization. See [Authentication Guide](authentication.md) for full details.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-enabled` | bool | `false` | Enable JWT authentication and scope-based authorization |
| `--auth-issuer` | string | `""` | OIDC issuer URL (used for discovery and token validation) |
| `--auth-service` | string | `""` | Service name prefix for scopes (e.g., `ledger` for `ledger:read`) |
| `--auth-read-key-set-max-retries` | int | `10` | Maximum retries when fetching the JWKS key set |
| `--auth-ed25519-keys` | string | `""` | Path to JSON file with Ed25519 public keys and scopes (auto-enables auth unless `--auth-enabled=false` is explicit) |

```bash
# Start server with OIDC authentication
ledger-v3-poc run \
  --auth-enabled \
  --auth-issuer https://auth.example.com \
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
| `LEDGERCTL_PROFILE` | `--profile` |
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

---

### upgrade

Self-update `ledgerctl` to the latest version from GitHub releases. Downloads the archive, verifies the SHA256 checksum, and replaces the binary in-place.

```bash
ledgerctl upgrade [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--channel` | `nightly` | Release channel: `nightly` or `stable` |
| `--force` | `false` | Upgrade even if already on the latest version |
| `--dry-run` | `false` | Check for updates without installing |

**Behavior:**
- Downloads the latest release from `formancehq/ledger-v3-poc` GitHub releases
- Verifies the archive's SHA256 checksum against `checksums.txt`
- Extracts the `ledgerctl` binary and atomically replaces the current binary
- If the current version is `dev` (built without ldflags), warns and requires `--force`

**Channels:**
- **`nightly`** (default): The rolling nightly build, tagged `nightly` on GitHub. Version format: `nightly-<shortcommit>`.
- **`stable`**: The latest tagged release matching `v*.*.*` semver format.

**Example:**

```bash
# Check for nightly updates (default channel)
ledgerctl upgrade --dry-run

# Upgrade to the latest nightly build
ledgerctl upgrade

# Upgrade to the latest stable release
ledgerctl upgrade --channel stable

# Force upgrade even if already up to date
ledgerctl upgrade --force
```

### query-checkpoint

Manage query checkpoints — coordinated snapshots of both the main store and the read index for point-in-time queries.

**Aliases:** `qcp`

**Subcommands:**

| Subcommand | Description |
|------------|-------------|
| `create` | Create a coordinated checkpoint of main and read index stores |
| `delete` | Delete a query checkpoint |
| `list` | List all query checkpoints |
| `info` | Show detailed information about a query checkpoint |
| `set-schedule` | Set a cron schedule for automatic checkpoint creation |
| `delete-schedule` | Remove the automatic checkpoint schedule |
| `get-schedule` | Show the current checkpoint schedule |

#### query-checkpoint create

Create a query checkpoint via Raft consensus. The checkpoint captures a physical Pebble snapshot of both the main store and the read index, enabling point-in-time queries.

```bash
ledgerctl query-checkpoint create [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--yaml` | `false` | Output as YAML |
| `--timeout` | `10s` | Request timeout |

**Behavior:**
- Routes through Raft so the checkpoint is replicated to all nodes
- The FSM commits pending state and creates a main store Pebble checkpoint; the read index checkpoint is created asynchronously by the index builder
- Checkpoints are stored under `{dataDir}/query-checkpoints/{id}/main/` and `{dataDir}/query-checkpoints/{id}/readindex/`
- Not cleaned up on restart — use `query-checkpoint delete` to remove

**Example:**

```bash
# Create a query checkpoint
ledgerctl query-checkpoint create

# Output as JSON (for scripting)
ledgerctl query-checkpoint create --json

# Short form
ledgerctl qcp create
```

#### query-checkpoint delete

Delete a previously created query checkpoint by its ID.

```bash
ledgerctl query-checkpoint delete <checkpoint-id>
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
ledgerctl query-checkpoint delete 1
```

#### query-checkpoint list

List all existing query checkpoints.

**Aliases:** `ls`

```bash
ledgerctl query-checkpoint list [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--yaml` | `false` | Output as YAML |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
ledgerctl query-checkpoint list
ledgerctl query-checkpoint list --json
ledgerctl qcp ls
```

#### query-checkpoint info

Show detailed information about a specific query checkpoint.

```bash
ledgerctl query-checkpoint info <checkpoint-id>
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | `false` | Output as JSON |
| `--yaml` | `false` | Output as YAML |
| `--timeout` | `10s` | Request timeout |

**Example:**

```bash
ledgerctl query-checkpoint info 1
```

#### query-checkpoint set-schedule

Set a cron schedule for automatic query checkpoint creation. The schedule is stored in Raft and takes effect immediately on the leader.

```bash
ledgerctl query-checkpoint set-schedule <cron-expression>
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

The cron expression uses the standard 5-field format (`minute hour day-of-month month day-of-week`) or the extended 6-field format with an optional leading seconds field (`second minute hour day-of-month month day-of-week`).

**Examples:**

```bash
# Create a checkpoint every day at midnight
ledgerctl query-checkpoint set-schedule "0 0 * * *"

# Create a checkpoint every hour
ledgerctl query-checkpoint set-schedule "0 * * * *"

# Create a checkpoint every 30 seconds (6-field format)
ledgerctl query-checkpoint set-schedule "*/30 * * * * *"
```

#### query-checkpoint delete-schedule

Remove the cron schedule for automatic query checkpoint creation, disabling automatic creation.

```bash
ledgerctl query-checkpoint delete-schedule
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

**Example:**

```bash
ledgerctl query-checkpoint delete-schedule
#  SUCCESS  Query checkpoint schedule deleted
```

#### query-checkpoint get-schedule

Display the current cron schedule for automatic query checkpoint creation.

```bash
ledgerctl query-checkpoint get-schedule
```

| Flag | Required | Description |
|------|----------|-------------|
| `--timeout` | No | Request timeout (default: 10s) |

**Example:**

```bash
ledgerctl query-checkpoint get-schedule

# Output (schedule set):
#  SUCCESS  Query checkpoint schedule: 0 0 * * *

# Output (no schedule):
#  SUCCESS  No query checkpoint schedule configured (automatic creation disabled)
```
