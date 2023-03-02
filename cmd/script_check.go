package cmd

import (
	"fmt"
	"os"

	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/spf13/cobra"
)

func NewScriptCheck() *cobra.Command {
	return &cobra.Command{
		Use:  "check [script]",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}

			_, err = compiler.Compile(string(b))
			if err != nil {
				return err
			} else {
				fmt.Println("Script is correct ✅")
			}
			return nil
		},
	}
}
