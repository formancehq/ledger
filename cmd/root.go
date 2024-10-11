package cmd

import (
	"github.com/formancehq/go-libs/bun/bunmigrate"
	"github.com/formancehq/go-libs/service"
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
	root.AddCommand(bunmigrate.NewDefaultCommand(func(cmd *cobra.Command, _ []string, _ *bun.DB) error {
		// todo: use provided db ...
		return upgradeAll(cmd)
	}))

	return root
}

func Execute() {
	service.Execute(NewRootCommand())
}
