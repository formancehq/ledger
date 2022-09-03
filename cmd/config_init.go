package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewConfigInit() *cobra.Command {
	return &cobra.Command{
		Use: "init",
		RunE: func(cmd *cobra.Command, args []string) error {
			return viper.SafeWriteConfig()
		},
	}
}
