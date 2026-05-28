package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_incremental_backup")

	ctx := context.Background()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)
		return
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	// 1. Run an incremental backup (exports log/audit entries since last export).
	resp, err := client.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
		Driver:     "s3",
		S3Bucket:   "backups",
		S3Region:   "us-east-1",
		S3Endpoint: "http://minio:9000",
	})
	if err != nil {
		if internal.IsTransient(err) {
			log.Printf("IncrementalBackup transient error: %v", err)
			return
		}

		// External service errors (S3 connectivity) are acceptable under chaos.
		if internal.IsExternalServiceError(err) {
			log.Printf("IncrementalBackup external service error: %v", err)
			return
		}

		assert.Unreachable("IncrementalBackup returned unexpected error",
			internal.Details{"error": err})

		return
	}

	details := internal.Details{
		"logEntriesExported":   resp.GetLogEntriesExported(),
		"auditEntriesExported": resp.GetAuditEntriesExported(),
		"segmentsUploaded":     resp.GetSegmentsUploaded(),
		"durationMs":           resp.GetDurationMs(),
		"lastLogSequence":      resp.GetLastLogSequence(),
		"lastAuditSequence":    resp.GetLastAuditSequence(),
	}

	assert.Reachable("incremental backup completed", details)

	// 2. Run a second incremental backup immediately.
	//    Should succeed with fewer or zero new entries.
	resp2, err := client.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
		Driver:     "s3",
		S3Bucket:   "backups",
		S3Region:   "us-east-1",
		S3Endpoint: "http://minio:9000",
	})
	if err != nil {
		if internal.IsTransient(err) || internal.IsExternalServiceError(err) {
			return
		}

		assert.Unreachable("second IncrementalBackup returned unexpected error",
			internal.Details{"error": err})

		return
	}

	// The second backup's last sequence should not regress below the first.
	assert.AlwaysOrUnreachable(resp2.GetLastLogSequence() >= resp.GetLastLogSequence(),
		"incremental backup log sequence should not regress",
		details.With(internal.Details{
			"firstLastLogSeq":  resp.GetLastLogSequence(),
			"secondLastLogSeq": resp2.GetLastLogSequence(),
		}))

	assert.AlwaysOrUnreachable(resp2.GetLastAuditSequence() >= resp.GetLastAuditSequence(),
		"incremental backup audit sequence should not regress",
		details.With(internal.Details{
			"firstLastAuditSeq":  resp.GetLastAuditSequence(),
			"secondLastAuditSeq": resp2.GetLastAuditSequence(),
		}))

	log.Printf("IncrementalBackup: exported %d logs, %d audit entries (%dms)",
		resp.GetLogEntriesExported(), resp.GetAuditEntriesExported(), resp.GetDurationMs())
}
