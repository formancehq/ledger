package cmd

import (
	"fmt"
	"os"

	"github.com/formancehq/stack/libs/go-libs/bun/bunmigrate"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/uptrace/bun"

	"github.com/formancehq/stack/libs/go-libs/aws/iam"
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/formancehq/ledger/cmd/internal"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlpmetrics"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	bindFlag = "bind"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func NewRootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "ledger",
		Short:             "ledger",
		DisableAutoGenTag: true,
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

	root.PersistentFlags().Bool(service.DebugFlag, false, "Debug mode")
	root.PersistentFlags().Bool(logging.JsonFormattingLoggerFlag, true, "Json formatting mode for logger")
	root.PersistentFlags().String(bindFlag, "0.0.0.0:3068", "API bind address")

	otlpmetrics.InitOTLPMetricsFlags(root.PersistentFlags())
	otlptraces.InitOTLPTracesFlags(root.PersistentFlags())
	auth.InitAuthFlags(root.PersistentFlags())
	publish.InitCLIFlags(root, func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.Schema
	})
	bunconnect.InitFlags(root.PersistentFlags())
	iam.InitFlags(root.PersistentFlags())

	if err := bindFlagsToViper(root); err != nil {
		panic(err)
	}

	internal.BindEnv(viper.GetViper())

	return root
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		if _, err := fmt.Fprintln(os.Stderr, err); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}
