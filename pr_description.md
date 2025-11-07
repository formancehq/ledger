## Description

This PR implements a complete bucket lifecycle management system, including soft deletion, restoration, and automatic hard deletion after a retention period. This allows for safe bucket management with the ability to recover accidentally deleted buckets.

## Changes

### Core Features

#### 1. Soft Deletion of Buckets
- **New endpoint**: `DELETE /v2/_/buckets/{bucket}`
  - Marks all ledgers within the specified bucket as deleted (soft delete)
  - Sets `deleted_at` timestamp for all ledgers in the bucket
  - Does not physically remove data, allowing for recovery

#### 2. Bucket Restoration
- **New endpoint**: `POST /v2/_/buckets/{bucket}/restore`
  - Restores a previously deleted bucket
  - Sets `deleted_at` to `NULL` for all ledgers in the bucket
  - Allows recovery of accidentally deleted buckets

#### 3. List Deleted Ledgers
- **Enhanced endpoint**: `GET /v2` (List Ledgers)
  - New query parameter: `includeDeleted` (boolean)
  - When `true`, includes soft-deleted ledgers in the response
  - When `false` or omitted, only returns active ledgers (default behavior)

#### 4. Bucket Cleanup Worker
- **Background worker**: `BucketCleanupRunner` that performs hard deletion after retention period
  - Periodically checks for buckets deleted longer than the retention period
  - Drops the database schema for expired buckets (CASCADE removes all objects)
  - Removes all ledger entries from `_system.ledgers` for expired buckets
  - Configurable via CLI flags:
    - `--worker-bucket-cleanup-retention-period`: Retention period before hard delete (default: 30 days)
    - `--worker-bucket-cleanup-schedule`: Cron schedule for worker execution (default: hourly)

### API Enhancements
- **Added `deletedAt` field to Ledger API responses**: The `V2Ledger` schema now includes a `deletedAt` field (nullable date-time) to indicate when a ledger was soft-deleted
  - Updated OpenAPI specification (`openapi.yaml` and `openapi/v2.yaml`)
  - Regenerated SDK to include the new field

### Database Changes
- **Migration**: Added `deleted_at` column to `_system.ledgers` table
  - Type: `timestamp`, nullable
  - Allows tracking when a ledger was soft-deleted

### Testing

#### Unit Tests
- **Bucket deletion**: `internal/api/v2/controllers_buckets_delete_test.go`
  - Tests successful deletion
  - Tests error handling

- **Bucket restoration**: `internal/api/v2/controllers_buckets_restore_test.go`
  - Tests successful restoration
  - Tests error handling

- **Worker logic**: `internal/storage/worker_bucket_cleanup_test.go`
  - Tests for non-deleted buckets (should not be deleted)
  - Tests for recently deleted buckets (should not be hard deleted)
  - Tests for old deleted buckets (should be hard deleted)
  - Tests for multiple buckets with different states

#### Integration Tests
- **Bucket lifecycle**: `test/e2e/api_buckets_delete_test.go`
  - Tests bucket deletion with multiple ledgers
  - Tests `includeDeleted` query parameter behavior
  - Tests bucket restoration
  - Tests listing deleted ledgers

- **Worker execution**: `test/e2e/app_bucket_cleanup_test.go`
  - Tests worker execution with real server and worker processes
  - Verifies bucket cleanup using API calls (no direct database access for verification)
  - Tests multiple buckets scenarios

### Configuration
- New CLI flags for worker:
  - `--worker-bucket-cleanup-retention-period`: Retention period before hard delete (default: 30 days)
  - `--worker-bucket-cleanup-schedule`: Cron schedule for worker execution (default: hourly)

### Infrastructure
- Added instrumentation functions in `pkg/testserver/worker.go` for test configuration
- Integrated worker module into the main worker module system
- Updated system store to support bucket operations (delete, restore, hard delete)

## Technical Details

### Soft Delete Flow
1. User calls `DELETE /v2/_/buckets/{bucket}`
2. System sets `deleted_at = NOW()` for all ledgers in the bucket
3. Ledgers are filtered out from normal queries (unless `includeDeleted=true`)
4. Data remains in database for recovery

### Restoration Flow
1. User calls `POST /v2/_/buckets/{bucket}/restore`
2. System sets `deleted_at = NULL` for all ledgers in the bucket
3. Ledgers become visible again in normal queries

### Hard Delete Flow (Worker)
1. Worker runs on configurable cron schedule
2. Queries for buckets where `deleted_at < (NOW() - retention_period)`
3. For each expired bucket:
   - Executes `DROP SCHEMA IF EXISTS {bucket} CASCADE`
   - Deletes all rows from `_system.ledgers` where `bucket = {bucket}`
4. Uses OpenTelemetry tracing for observability

## Testing

All tests pass:
- Unit tests for API handlers
- Unit tests for worker logic
- Integration tests for bucket lifecycle
- Integration tests with real server and worker processes
- Existing tests remain unaffected

## Migration Notes

- A new migration adds the `deleted_at` column to `_system.ledgers`
- Existing ledgers will have `deleted_at = NULL` (not deleted)
- No data loss during migration
