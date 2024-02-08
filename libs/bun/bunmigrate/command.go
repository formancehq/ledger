package bunmigrate

import (
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	sharedlogging "github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"

	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"

	// Import the postgres driver.
	_ "github.com/lib/pq"
)

type Executor func(cmd *cobra.Command, args []string, db *bun.DB) error

func NewDefaultCommand(executor Executor, options ...func(command *cobra.Command)) *cobra.Command {
	ret := &cobra.Command{
		Use:   "migrate",
		Short: "Run migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(cmd, args, executor)
		},
	}
	for _, option := range options {
		option(ret)
	}
	bunconnect.InitFlags(ret.Flags())
	return ret
}

func Run(cmd *cobra.Command, args []string, executor Executor) error {
	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag))
	if err != nil {
		return errors.Wrap(err, "evaluating connection options")
	}

	db, err := bunconnect.OpenSQLDB(*connectionOptions)
	if err != nil {
		return errors.Wrap(err, "opening database")
	}
	defer func() {
		err := db.Close()
		if err != nil {
			sharedlogging.FromContext(cmd.Context()).Errorf("Closing database: %s", err)
		}
	}()
	if viper.GetBool(service.DebugFlag) {
		db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithWriter(cmd.OutOrStdout())))
	}

	return errors.Wrap(executor(cmd, args, db), "executing migration")
}
