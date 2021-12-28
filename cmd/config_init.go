package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewConfigInit() *cobra.Command {
	return &cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			err := viper.SafeWriteConfig()
			if err != nil {
				logrus.Println(err)
			}
		},
	}
}
