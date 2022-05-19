package cmd

import (
	"fmt"
	"io/ioutil"

	"github.com/numary/machine/script/compiler"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewScriptCheck() *cobra.Command {
	return &cobra.Command{
		Use:  "check [script]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			b, err := ioutil.ReadFile(args[0])
			if err != nil {
				logrus.Fatal(err)
			}

			_, err = compiler.Compile(string(b))
			if err != nil {
				logrus.Fatal(err)
			} else {
				fmt.Println("Script is correct ✅")
			}
		},
	}
}
