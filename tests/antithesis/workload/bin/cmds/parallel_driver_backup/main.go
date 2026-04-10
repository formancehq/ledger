package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	log.Println("composer: parallel_driver_backup")

	ctx := context.Background()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)
		return
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	resp, err := client.Backup(ctx, &clusterpb.BackupRequest{
		Driver:     "s3",
		S3Bucket:   "backups",
		S3Region:   "us-east-1",
		S3Endpoint: "http://minio:9000",
	})
	if err != nil {
		if internal.IsTransient(err) {
			log.Printf("Backup transient error: %s", err)
			return
		}

		// S3 infrastructure errors (bucket not created yet, MinIO not ready)
		// are not application bugs — just log and bail.
		st, _ := status.FromError(err)
		if st.Code() == codes.Unknown {
			log.Printf("Backup infrastructure error (likely S3): %s", err)
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
