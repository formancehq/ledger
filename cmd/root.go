package cmd

import (
	"github.com/formancehq/go-libs/bun/bunmigrate"
	"github.com/formancehq/go-libs/service"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/aws/iam"
	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/ledger/v2/internal/storage/systemstore"
	"github.com/spf13/cobra"
)

const (
	BindFlag = "bind"
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

	serve := NewServe()
	version := NewVersion()

	buckets := NewBucket()
	buckets.AddCommand(NewBucketUpgrade())

	root.AddCommand(serve)
	root.AddCommand(buckets)
	root.AddCommand(version)
	root.AddCommand(bunmigrate.NewDefaultCommand(func(cmd *cobra.Command, args []string, db *bun.DB) error {
		return upgradeAll(cmd, args)
	}))

	root.AddCommand(NewDocCommand())

	root.PersistentFlags().String(BindFlag, "0.0.0.0:3068", "API bind address")

	service.AddFlags(root.PersistentFlags())
	otlpmetrics.AddFlags(root.PersistentFlags())
	otlptraces.AddFlags(root.PersistentFlags())
	auth.AddFlags(root.PersistentFlags())
	publish.AddFlags(ServiceName, root.PersistentFlags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.Schema
	})
	bunconnect.AddFlags(root.PersistentFlags())
	iam.AddFlags(root.PersistentFlags())

	return root
}

func Execute() {
	service.Execute(NewRootCommand())
}
