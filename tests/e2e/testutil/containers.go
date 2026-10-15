package testutil

const (
	// AzuriteImage is pinned because the full-tag E2E suite is a required CI
	// gate. Mutable latest tags would let an external release break that gate.
	AzuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.36.0"

	// ClickHouseImage is pinned for the same reason as AzuriteImage: the
	// clickhouse-tagged suite is part of the required full-tag CI gate, and the
	// mutable 24-alpine series tag could move under it at any time. This is the
	// exact version that tag resolves to today (v24.10.2.80-stable, identical
	// amd64 manifest), so pinning it changes nothing the gate already runs.
	ClickHouseImage = "clickhouse/clickhouse-server:24.10.2.80-alpine"

	// MinIOImage is Formance's mirror of a MinIO build, pinned by digest. The
	// upstream minio/minio Docker Hub repository was removed, so every S3
	// emulator in the S3-tagged E2E suites uses this mirror. Keep it aligned
	// with backupMinioImage in internal/infra/backup/s3_test.go.
	MinIOImage = "ghcr.io/formancehq/minio:RELEASE.2025-10-15T17-29-55Z@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9"
)
