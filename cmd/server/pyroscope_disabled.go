//go:build !pyroscope

package server

import (
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/pyroscope"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

func addPyroscopeFlags(_ *flag.FlagSet) {}

func pyroscopeConfigFromFlags(_ *cobra.Command) pyroscope.Config {
	return pyroscope.Config{}
}
