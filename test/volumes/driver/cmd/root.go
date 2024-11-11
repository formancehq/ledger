package cmd

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/tests/volumes/driver/internal"
	"github.com/spf13/cobra"
	"log"
	"slices"
)

func run(cmd *cobra.Command, _ []string) error {

	stepsAsUInt, err := cmd.Flags().GetUintSlice(stepFlag)
	if err != nil {
		return err
	}

	ledgerIP, err := cmd.Flags().GetString(ledgerIPFlag)
	if err != nil {
		return err
	}
	if ledgerIP == "" {
		return fmt.Errorf("missing --" + ledgerIPFlag)
	}

	dbClusterIdentifier, err := cmd.Flags().GetString(dbClusterIdentifierFlag)
	if err != nil {
		return err
	}
	if dbClusterIdentifier == "" {
		return fmt.Errorf("missing --" + dbClusterIdentifierFlag)
	}

	s3Bucket, err := cmd.Flags().GetString(s3BucketFlag)
	if err != nil {
		return err
	}
	if s3Bucket == "" {
		return fmt.Errorf("missing --" + s3BucketFlag)
	}

	steps := collectionutils.Map(stepsAsUInt, func(from uint) uint64 {
		return uint64(from)
	})

	slices.Sort(steps)

	sdkConfig, err := config.LoadDefaultConfig(cmd.Context())
	if err != nil {
		return fmt.Errorf("resolving aws config: %w", err)
	}

	vus, err := cmd.Flags().GetInt(vusFlag)
	if err != nil {
		return fmt.Errorf("resolving vus: %w", err)
	}

	return internal.
		NewRunner(
			logging.FromContext(cmd.Context()),
			ledgerIP,
			steps,
			sdkConfig,
			s3Bucket,
			dbClusterIdentifier,
			vus,
		).
		Run(cmd.Context())
}

var rootCmd = &cobra.Command{
	Use:          "feeder",
	RunE:         run,
	SilenceUsage: true,
}

const (
	ledgerIPFlag            = "ledger-ip"
	dbClusterIdentifierFlag = "db-cluster-identifier"
	stepFlag                = "step"
	vusFlag                 = "vus"
	s3BucketFlag            = "s3-bucket"
)

func Execute() {
	rootCmd.Flags().UintSliceP(stepFlag, "s", []uint{}, "steps")
	rootCmd.Flags().String(ledgerIPFlag, "", "Ledger IP")
	rootCmd.Flags().String(dbClusterIdentifierFlag, "", "Database cluster identifier")
	rootCmd.Flags().String(s3BucketFlag, "", "S3 bucket")
	rootCmd.Flags().Int(vusFlag, 50, "Number of virtual users")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
