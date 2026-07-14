package main

import (
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func s3Storage() *commonpb.BackupStorage {
	return &commonpb.BackupStorage{
		Provider: &commonpb.BackupStorage_S3{
			S3: &commonpb.S3StorageConfig{
				Bucket:   "backups",
				Region:   "us-east-1",
				Endpoint: "http://minio:9000",
			},
		},
	}
}

func main() {
	log.Println("composer: parallel_driver_incremental_backup")

	ctx, cancel := internal.SingletonContext()
	defer cancel()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)
		return
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	// 0. Establish a full checkpoint first. An incremental backup is only
	//    meaningful layered on a full checkpoint (EN-888): the checkpoint carries
	//    the Global-zone persisted config, last-applied index, and timestamp that
	//    restore needs, so the server now rejects an incremental against a
	//    checkpoint-less destination with FailedPrecondition. Antithesis schedules
	//    drivers randomly, so this driver cannot assume the full-backup driver ran
	//    first — it takes its own full backup to guarantee the precondition holds.
	if _, err := client.Backup(ctx, &clusterpb.BackupRequest{Storage: s3Storage()}); err != nil {
		if internal.IsTransient(err) {
			log.Printf("Backup (pre-incremental) transient error: %v", err)
			return
		}

		assert.Unreachable("Backup (pre-incremental) returned unexpected error",
			internal.Details{"error": err})

		return
	}

	// 1. Run an incremental backup (exports log/audit entries since last export).
	resp, err := client.IncrementalBackup(ctx, &clusterpb.IncrementalBackupRequest{
		Storage: s3Storage(),
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

		// A concurrent restore/backup on the same destination can remove the
		// checkpoint between step 0 and here; the no-full-checkpoint precondition
		// is then an expected, acceptable outcome — not a finding (EN-888).
		if internal.IsNoFullCheckpoint(err) {
			log.Printf("IncrementalBackup: no full checkpoint at destination (acceptable): %v", err)
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
		Storage: s3Storage(),
	})
	if err != nil {
		if internal.IsTransient(err) || internal.IsExternalServiceError(err) ||
			internal.IsNoFullCheckpoint(err) {
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
