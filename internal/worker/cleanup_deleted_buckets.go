package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	internal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
)

type CleanupDeletedBucketsConfig struct {
	RetentionDays int
	Schedule      cron.Schedule
}

type CleanupDeletedBucketsRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	db          *bun.DB
	systemStore system.Store
	cfg         CleanupDeletedBucketsConfig
}

func (r *CleanupDeletedBucketsRunner) Name() string {
	return "Cleanup deleted buckets"
}

func (r *CleanupDeletedBucketsRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running cleanup deleted buckets: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *CleanupDeletedBucketsRunner) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

func (r *CleanupDeletedBucketsRunner) run(ctx context.Context) error {
	// Calculate the cutoff date (ledgers deleted before this date will be physically deleted)
	cutoffDate := time.Now().AddDate(0, 0, -r.cfg.RetentionDays)

	r.logger.Infof("Cleaning up ledgers in deleted buckets before %s", cutoffDate.Format(time.RFC3339))

	// Start a single transaction for the entire cleanup process
	return r.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Get all buckets with deleted ledgers past the retention period
		var bucketsToProcess []string
		err := tx.NewRaw(`
			SELECT DISTINCT bucket FROM _system.ledgers
			WHERE deleted_at IS NOT NULL AND deleted_at < ?
		`, cutoffDate).Scan(ctx, &bucketsToProcess)

		if err != nil {
			return fmt.Errorf("error finding buckets to process: %w", err)
		}

		r.logger.Infof("Found %d buckets with deleted ledgers to process", len(bucketsToProcess))

		// Process each bucket within the same transaction
		for _, bucketName := range bucketsToProcess {
			// Check if bucket name format is valid
			if !internal.BucketNameFormat().MatchString(bucketName) {
				return fmt.Errorf("invalid bucket name format: must match '%s'", internal.BucketNameFormat().String())
			}

			// Check if bucket name is reserved
			for _, reserved := range internal.ReservedBucketNames() {
				if bucketName == reserved {
					return fmt.Errorf("cannot delete reserved bucket: %s", bucketName)
				}
			}

			r.logger.Infof("Processing bucket: %s", bucketName)

			// Get all ledgers in this bucket that need to be deleted
			var ledgersToDelete []struct {
				Name string
			}

			err := tx.NewRaw(`
				SELECT name FROM _system.ledgers
				WHERE bucket = ? AND deleted_at IS NOT NULL AND deleted_at < ?
			`, bucketName, cutoffDate).Scan(ctx, &ledgersToDelete)

			if err != nil {
				return fmt.Errorf("error finding ledgers in bucket %s: %w", bucketName, err)
			}

			r.logger.Infof("Found %d ledgers to delete in bucket %s", len(ledgersToDelete), bucketName)

			// Process each ledger in the bucket within the same transaction
			for _, ledger := range ledgersToDelete {
				r.logger.Infof("Physically deleting ledger: %s", ledger.Name)

				// 1. Drop the schema for this ledger
				_, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, ledger.Name))
				if err != nil {
					return fmt.Errorf("error dropping ledger schema %s: %w", ledger.Name, err)
				}

				// 2. Delete the ledger record from _system.ledgers
				_, err = tx.ExecContext(ctx, `
					DELETE FROM _system.ledgers
					WHERE name = ?
				`, ledger.Name)
				if err != nil {
					return fmt.Errorf("error deleting ledger record %s: %w", ledger.Name, err)
				}
			}

			r.logger.Infof("Successfully processed bucket: %s", bucketName)
		}

		return nil
	})
}

func NewCleanupDeletedBucketsRunner(logger logging.Logger, db *bun.DB, systemStore system.Store, cfg CleanupDeletedBucketsConfig) *CleanupDeletedBucketsRunner {
	return &CleanupDeletedBucketsRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		systemStore: systemStore,
		cfg:         cfg,
	}
}
