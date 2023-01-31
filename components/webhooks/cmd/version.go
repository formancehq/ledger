package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	ServiceName = "webhooks"
	Version     = "develop"
	BuildDate   = "-"
	Commit      = "-"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: fmt.Sprintf("Get %s version", ServiceName),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %s \n", Version)
		fmt.Printf("Date: %s \n", BuildDate)
		fmt.Printf("Commit: %s \n", Commit)
	},
}
