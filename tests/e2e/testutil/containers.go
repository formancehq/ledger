package testutil

const (
	// AzuriteImage is pinned because the full-tag E2E suite is a required CI
	// gate. Mutable latest tags would let an external release break that gate.
	AzuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.36.0"

	// MinIOImage matches the version used by the Antithesis fixtures and keeps
	// every S3-tagged E2E test on one reproducible emulator version.
	MinIOImage = "minio/minio:RELEASE.2024-06-13T22-53-53Z"
)
