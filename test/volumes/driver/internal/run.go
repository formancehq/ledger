package internal

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/formancehq/ledger/pkg/generate"
	"net/http"
	"time"
)

//go:embed script.js
var script string

type Runner struct {
	ledgerIP            string
	steps               []uint64
	rdsClient           *rds.Client
	dbClusterIdentifier string
	client              *client.Formance
	s3Client            *s3.Client
	s3Bucket            string
	logger              logging.Logger
	vus                 int
}

func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("Create ledger")
	if err := r.createLedger(ctx); err != nil {
		return err
	}

	for i, step := range r.steps {
		r.logger.Infof("Run step %d...", i)

		r.logger.Infof("Run generator")
		if err := r.runGenerator(ctx, step); err != nil {
			return err
		}

		r.logger.Info("Create database snapshot")
		if err := r.createDBSnapshot(ctx, step); err != nil {
			return err
		}

		r.logger.Info("Export metrics")
		if err := r.exportMetrics(ctx, step); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) createDBSnapshot(ctx context.Context, step uint64) error {

	dbClusterIdentifier := aws.String(r.dbClusterIdentifier)
	dbSnapshotIdentifier := aws.String(fmt.Sprintf("%s-%d", r.dbClusterIdentifier, step))

	snapshots, err := r.rdsClient.DescribeDBClusterSnapshots(ctx, &rds.DescribeDBClusterSnapshotsInput{
		DBClusterIdentifier:         dbClusterIdentifier,
		DBClusterSnapshotIdentifier: dbSnapshotIdentifier,
	})
	if err != nil {
		return fmt.Errorf("describing db cluster snapshots: %w", err)
	}

	if len(snapshots.DBClusterSnapshots) > 0 {
		r.logger.Info("Snapshot already exists")
	} else {
		_, err = r.rdsClient.CreateDBClusterSnapshot(ctx, &rds.CreateDBClusterSnapshotInput{
			DBClusterIdentifier:         dbClusterIdentifier,
			DBClusterSnapshotIdentifier: dbSnapshotIdentifier,
		})
		if err != nil {
			return fmt.Errorf("creating db snapshot: %w", err)
		}
	}

	for {
		r.logger.Info("Checking snapshot status")

		snapshots, err := r.rdsClient.DescribeDBClusterSnapshots(ctx, &rds.DescribeDBClusterSnapshotsInput{
			DBClusterIdentifier:         dbClusterIdentifier,
			DBClusterSnapshotIdentifier: dbSnapshotIdentifier,
		})
		if err != nil {
			return fmt.Errorf("describing db cluster snapshots: %w", err)
		}

		if *snapshots.DBClusterSnapshots[0].Status == "available" {
			r.logger.Info("Snapshot available")
			break
		} else {
			r.logger.Infof("Snapshot status = %s", *snapshots.DBClusterSnapshots[0].Status)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}

	return err
}

func (r *Runner) runGenerator(ctx context.Context, step uint64) error {
	r.logger.Infof("Running generator with %d vus", r.vus)
	generatorSet := generate.NewGeneratorSet(r.vus, script, "default", r.client, step)
	if err := generatorSet.Run(ctx); err != nil {
		return fmt.Errorf("running generator: %s", err)
	}

	return nil
}

func (r *Runner) exportMetrics(ctx context.Context, step uint64) error {
	metrics, err := r.client.Ledger.GetMetrics(ctx)
	if err != nil {
		return fmt.Errorf("getting metrics: %w", err)
	}

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(metrics.Object); err != nil {
		return fmt.Errorf("encoding metrics: %w", err)
	}

	_, err = r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.s3Bucket,
		Key:    pointer.For(fmt.Sprintf("step-%d", step)),
	})
	if err != nil {
		r.logger.Infof("Unable to get s3 object: %s", err)

		_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &r.s3Bucket,
			Key:    pointer.For(fmt.Sprintf("step-%d", step)),
			Body:   buf,
		})
		if err != nil {
			return fmt.Errorf("putting object: %w", err)
		}
	}

	return nil
}

func (r *Runner) createLedger(ctx context.Context) error {
	_, err := r.client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: "default",
		V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
			Features: features.MinimalFeatureSet,
		},
	})
	if err != nil {
		if err, ok := err.(*sdkerrors.V2ErrorResponse); ok {
			if err.ErrorCode == components.V2ErrorsEnumLedgerAlreadyExists {
				r.logger.Info("Ledger already exists")
				return nil
			}
		}
		return fmt.Errorf("creating ledger: %w", err)
	}

	return nil
}

func NewRunner(
	logger logging.Logger,
	ledgerIP string,
	steps []uint64,
	sdkConfig aws.Config,
	s3Bucket string,
	dbClusterIdentifier string,
	vus int,
) *Runner {
	return &Runner{
		ledgerIP:            ledgerIP,
		steps:               steps,
		rdsClient:           rds.NewFromConfig(sdkConfig),
		s3Client:            s3.NewFromConfig(sdkConfig),
		s3Bucket:            s3Bucket,
		dbClusterIdentifier: dbClusterIdentifier,
		client: client.New(
			client.WithServerURL(fmt.Sprintf("http://%s:8080", ledgerIP)),
			client.WithClient(&http.Client{
				Timeout: time.Hour,
			}),
		),
		logger: logger,
		vus:    vus,
	}
}
