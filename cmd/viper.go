package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func bindFlagsToViper(cmd *cobra.Command) error {
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}

	return viper.BindPFlags(cmd.PersistentFlags())
}
