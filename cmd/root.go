package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunmigrate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/spf13/cobra"
	"github.com/uptrace/bun"
)

const (
	ServiceName = "ledger"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:               "ledger",
		Short:             "ledger",
		DisableAutoGenTag: true,
		Version:           Version,
	}

	root.AddCommand(NewServeCommand())
	root.AddCommand(NewBucketsCommand())
	root.AddCommand(NewVersionCommand())
	root.AddCommand(NewWorkerCommand())
	root.AddCommand(NewDocsCommand())

	root.AddCommand(bunmigrate.NewDefaultCommand(func(cmd *cobra.Command, _ []string, db *bun.DB) error {
		logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)
		cmd.SetContext(logging.ContextWithLogger(cmd.Context(), logger))

		driver := driver.New(db, ledger.NewFactory(db), bucket.NewDefaultFactory())
		if err := driver.Initialize(cmd.Context()); err != nil {
			return err
		}

		return driver.UpgradeAllBuckets(cmd.Context())
	}))
	root.AddCommand(NewDocsCommand())
	service.AddFlags(root.PersistentFlags())

	return root
}

func Execute() {
	service.Execute(NewRootCommand())
}
