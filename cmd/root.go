package cmd

import (
	"github.com/spf13/cobra"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/observe"
	"github.com/formancehq/go-libs/v5/pkg/observe/traces"
	"github.com/formancehq/go-libs/v5/pkg/service"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/migrate"

	"github.com/formancehq/ledger/internal/storage/driver"
)

const (
	ServiceName = "ledger"

	NumscriptInterpreterFlag        = "experimental-numscript-interpreter"
	NumscriptInterpreterFlagsToPass = "experimental-numscript-interpreter-flags"
	ExperimentalFeaturesFlag        = "experimental-features"
	ExperimentalExporters           = "experimental-exporters"
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

	root.PersistentFlags().Bool(ExperimentalFeaturesFlag, false, "Enable features configurability")
	root.PersistentFlags().Bool(ExperimentalExporters, false, "Enable exporters support")

	root.AddCommand(NewServeCommand())
	root.AddCommand(NewBucketsCommand())
	root.AddCommand(NewVersionCommand())
	root.AddCommand(NewWorkerCommand())
	root.AddCommand(NewDocsCommand())

	root.AddCommand(newMigrationCommand())
	root.AddCommand(NewDocsCommand())

	service.AddFlags(root.PersistentFlags())

	return root
}

func newMigrationCommand() *cobra.Command {
	ret := migrate.NewDefaultCommand(func(cmd *cobra.Command, _ []string, db *bun.DB) error {
		return withStorageDriver(cmd, func(driver *driver.Driver) error {
			if err := driver.Initialize(cmd.Context()); err != nil {
				return err
			}

			return driver.UpgradeAllBuckets(cmd.Context())
		})
	})
	observe.AddFlags(ret.Flags())
	traces.AddFlags(ret.Flags())

	return ret
}

func Execute() {
	service.Execute(NewRootCommand())
}
