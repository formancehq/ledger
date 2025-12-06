# AGENTS.md - CLI Client Structure

This document describes the organizational structure of the CLI client commands.

## File Structure

The client commands are organized into separate files to improve maintainability and code readability.

### Bucket Commands

Bucket management commands are separated into individual files:

- **`buckets.go`** : Main file that defines the parent `buckets` command and initializes all sub-commands
- **`buckets_create.go`** : `buckets create` command to create a new bucket
- **`buckets_list.go`** : `buckets list` command to list all buckets
- **`buckets_get.go`** : `buckets get` command to retrieve a bucket with its Raft state
- **`buckets_delete.go`** : `buckets delete` command to delete a bucket

### Ledger Commands

Ledger management commands are separated into individual files:

- **`ledgers.go`** : Main file that defines the parent `ledgers` command and initializes all sub-commands
- **`ledgers_create.go`** : `ledgers create` command to create a new ledger in a bucket
- **`ledgers_list.go`** : `ledgers list` command to list all ledgers in a bucket
- **`ledgers_get.go`** : `ledgers get` command to retrieve a specific ledger

### Shared Files

- **`main.go`** : Main entry point, defines `rootCmd` and initializes all commands
- **`common.go`** : Shared functions (SDK client creation, debug HTTP client)
- **`cluster.go`** : Raft cluster-related commands (`snapshot`, `cluster-state`)

## Conventions

1. **One file per command** : Each sub-command (create, list, get, delete) has its own file
2. **Parent file** : Each command group (buckets, ledgers) has a main file that defines the parent command and calls `init()` for each sub-command
3. **Global variables** : Flag variables are defined in the corresponding command file
4. **`init()` function** : Each command file uses `init()` to define its flags and mark required flags

## Example: Adding a New Command

To add a new `buckets update` command:

1. Create `buckets_update.go` with:
   - Global variables for flags
   - Definition of `bucketsUpdateCmd`
   - `init()` function to configure flags
   - `runUpdateBucket()` function for the implementation

2. Modify `buckets.go` to add:
   ```go
   bucketsCmd.AddCommand(bucketsUpdateCmd)
   ```

This structure enables easy maintenance and clear separation of responsibilities.
