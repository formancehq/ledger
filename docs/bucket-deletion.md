# Bucket Deletion Feature

## Overview

The Bucket Deletion feature allows users to delete buckets from the Formance Ledger system. This is a two-phase process:

1. **Soft Delete**: Buckets are first marked for deletion, making them inaccessible via the API.
2. **Physical Delete**: After a configurable grace period (default 30 days), buckets can be physically deleted from the system.

## Database Changes

The feature adds a `deleted_at` timestamp column to the ledger table in the system schema. When a bucket is marked for deletion, this timestamp is set to the current time. When a bucket is restored, this timestamp is cleared.

## API Endpoints

The following API endpoints are available for bucket management:

- `GET /v2/_/buckets` - List all buckets with their deletion status
- `DELETE /v2/_/buckets/{bucket}` - Mark a bucket for deletion
- `POST /v2/_/buckets/{bucket}/restore` - Restore a bucket that was marked for deletion

## CLI Commands

The following CLI commands are available for bucket management:

- `ledger buckets list` - List all buckets with their deletion status
- `ledger buckets delete [--days=30]` - Physically delete buckets that were marked for deletion N days ago
- `ledger buckets restore [bucket]` - Restore a bucket that was marked for deletion

## Usage Examples

### Marking a Bucket for Deletion

```bash
# Using the API
curl -X DELETE http://localhost:8080/v2/_/buckets/mybucket

# The bucket will no longer be accessible via the API
curl -X GET http://localhost:8080/v2/mybucket/accounts
# Returns 404 Not Found
```

### Listing Buckets with Deletion Status

```bash
# Using the API
curl -X GET http://localhost:8080/v2/_/buckets

# Using the CLI
ledger buckets list
```

### Restoring a Deleted Bucket

```bash
# Using the API
curl -X POST http://localhost:8080/v2/_/buckets/mybucket/restore

# Using the CLI
ledger buckets restore mybucket
```

### Physically Deleting Buckets

```bash
# Using the CLI to delete buckets marked for deletion 30 days ago
ledger buckets delete

# Using the CLI to delete buckets marked for deletion 7 days ago
ledger buckets delete --days=7
```

## Implementation Details

When a bucket is marked for deletion:
- All ledgers in that bucket become inaccessible
- The API stops responding on any `/v2/{ledger}` contained in a deleted bucket, acting as if the ledger did not exist
- The bucket can be restored through both API and CLI

The physical deletion process:
- Drops the bucket schema and all its contents
- Deletes the ledger entries for the bucket from the system store
- Is irreversible - once a bucket is physically deleted, it cannot be restored
