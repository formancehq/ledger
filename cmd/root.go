package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunmigrate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/uptrace/bun"

	"github.com/spf13/cobra"
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

	serve := NewServeCommand()
	version := NewVersion()

	buckets := NewBucket()

	root.AddCommand(serve)
	root.AddCommand(buckets)
	root.AddCommand(version)
	root.AddCommand(bunmigrate.NewDefaultCommand(func(cmd *cobra.Command, _ []string, db *bun.DB) error {
		logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)
		cmd.SetContext(logging.ContextWithLogger(cmd.Context(), logger))

		driver := driver.New(
			ledger.NewFactory(db),
			systemstore.New(db),
			bucket.NewDefaultFactory(db),
		)
		if err := driver.Initialize(cmd.Context()); err != nil {
			return err
		}

		return driver.UpgradeAllBuckets(cmd.Context())
	}))
	root.AddCommand(NewDocsCommand())

	return root
}

func Execute() {
	service.Execute(NewRootCommand())
}
