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

	// MinIOImage matches the version used by the Antithesis fixtures and keeps
	// every S3-tagged E2E test on one reproducible emulator version.
	MinIOImage = "minio/minio:RELEASE.2024-06-13T22-53-53Z"
)
