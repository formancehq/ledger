package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_backup")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)

		return
	}
	defer func() { _ = conn.Close() }()

	run(ctx, ledgerpb.NewClusterServiceClient(conn))
}

func run(ctx context.Context, client ledgerpb.ClusterServiceClient) {
	resp, err := internal.RetryBackup(ctx, "Backup", func(ctx context.Context) (*ledgerpb.BackupResponse, error) {
		return client.Backup(ctx, &ledgerpb.BackupRequest{
			Storage: &ledgerpb.BackupStorage{
				Provider: &ledgerpb.BackupStorage_S3{
					S3: &ledgerpb.S3StorageConfig{
						Bucket:   "backups",
						Region:   "us-east-1",
						Endpoint: "http://minio:9000",
					},
				},
			},
		})
	})
	if err != nil {
		if internal.IsBackupCallerCancellation(ctx, err) || internal.IsTransient(err) || internal.IsBackupInProgress(err) {
			log.Printf("Backup inconclusive error after retries: %s", err)

			return
		}

		assert.Unreachable("Backup returned unexpected error", internal.Details{"error": err})

		return
	}

	details := internal.Details{
		"filesUploaded": resp.GetFilesUploaded(),
		"totalFiles":    resp.GetTotalFiles(),
		"durationMs":    resp.GetDurationMs(),
	}

	assert.AlwaysOrUnreachable(resp.GetTotalFiles() > 0, "backup should produce files", details)

	assert.Reachable("backup completed successfully", details)
	log.Printf("Backup completed: %d uploaded, %d total (%dms)",
		resp.GetFilesUploaded(), resp.GetTotalFiles(), resp.GetDurationMs())
}
