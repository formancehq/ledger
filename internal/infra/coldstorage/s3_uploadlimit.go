//go:build s3

package coldstorage

import "context"

// s3UploadSlots bounds the number of concurrent S3 multipart uploads across
// every S3Storage in this process — both cold-storage chapter archives
// (Archive, below) and backup destination uploads (internal/infra/backup/s3.go
// PutFile, which calls AcquireS3UploadSlot since it already depends on this
// package for its S3 client — see NewS3BackupStorage).
//
// Sizing: each active multipart upload pools ~192 MiB steady-state (~224 MiB
// transient) at the production part size and SDK concurrency default (see
// s3UploadPartSize). The documented default in-memory budget is ~3.2 GiB per
// node (docs/ops/memory.md); the production-baseline deployment profile
// (GOMEMLIMIT=3600MiB, docs/ops/deployment-profiles.md) leaves under 400 MiB
// of headroom above that budget for "workload-dependent gRPC buffers and
// transient allocations" (deployment-profiles.md). Nothing else bounds
// concurrency across uploads: BackupJobsState excludes only per
// destination, and the Archiver can overlap with backups, so without a
// process-wide cap the number of concurrently active uploads — and
// therefore aggregate uploader memory — is unbounded.
//
// Fixed at 2 rather than left as an unbounded knob: 2 concurrent uploads is
// ~384 MiB steady-state, and up to ~448 MiB if both are transiently at
// their peak at once — deliberately traded against the production-baseline
// profile's <400 MiB of headroom for throughput (a single destination
// backing up while a chapter archives, or two destinations backing up at
// once, no longer serialize behind each other). Nodes running that profile
// close to the wire should either raise GOMEMLIMIT or watch the
// startup-logged memory estimate under concurrent backup+archive load.
const s3UploadSlots = 2

var s3UploadSem = make(chan struct{}, s3UploadSlots)

// AcquireS3UploadSlot blocks until a global S3 upload slot is free or ctx is
// done, bounding the number of concurrent S3 multipart uploads across the
// whole process (see s3UploadSlots). Call the returned release func exactly
// once, whether or not the upload succeeds.
func AcquireS3UploadSlot(ctx context.Context) (release func(), err error) {
	select {
	case s3UploadSem <- struct{}{}:
		return func() { <-s3UploadSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
