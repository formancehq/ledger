package bunmigrate

import (
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/spf13/cobra"
	"github.com/uptrace/bun"

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
	bunconnect.AddFlags(ret.Flags())
	return ret
}
