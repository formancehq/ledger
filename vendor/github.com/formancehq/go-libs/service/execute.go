package service

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func Execute(cmd *cobra.Command) {
	BindEnvToCommand(cmd)
	if err := cmd.Execute(); err != nil {
		if _, err := fmt.Fprintln(os.Stderr, err); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}
