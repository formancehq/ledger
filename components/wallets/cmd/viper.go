package cmd

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()
}

func bindFlagsToViper(cmd *cobra.Command) error {
	return errors.Wrap(viper.BindPFlags(cmd.Flags()), "binding viper flags")
}
