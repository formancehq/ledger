//go:build !pyroscope

package server

import (
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/pyroscope"
)

func addPyroscopeFlags(_ *flag.FlagSet) {}

func pyroscopeConfigFromFlags(_ *cobra.Command) pyroscope.Config {
	return pyroscope.Config{}
}
