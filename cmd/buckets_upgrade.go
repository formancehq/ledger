package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/spf13/cobra"
	"github.com/uptrace/bun"
)

func NewBucketUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)
			cmd.SetContext(logging.ContextWithLogger(cmd.Context(), logger))

			driver, db, err := getDriver(cmd)
			if err != nil {
				return err
			}
			defer func() {
				_ = db.Close()
			}()

			if args[0] == "*" {
				return driver.UpgradeAllBuckets(cmd.Context())
			}

			return driver.UpgradeBucket(cmd.Context(), args[0])
		},
	}

	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())

	return cmd
}

func getDriver(cmd *cobra.Command) (*driver.Driver, *bun.DB, error) {

	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
	if err != nil {
		return nil, nil, err
	}

	db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
	if err != nil {
		return nil, nil, err
	}

	driver := driver.New(
		ledger.NewFactory(db),
		systemstore.New(db),
		bucket.NewDefaultFactory(db),
	)
	if err := driver.Initialize(cmd.Context()); err != nil {
		return nil, nil, err
	}

	return driver, db, nil
}
