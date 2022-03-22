package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
)

func NewDocCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "doc",
	}
	cmd.AddCommand(NewDocFlagCommand())
	return cmd
}

func NewDocFlagCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "flags",
		Run: func(cmd *cobra.Command, args []string) {

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.Debug)
			defer w.Flush()

			allKeys := viper.GetViper().AllKeys()
			sort.Strings(allKeys)

			fmt.Fprintf(w, "\tFlag\tEnv var\tDefault value\tDescription\t\r\n")
			fmt.Fprintf(w, "\t-\t-\t-\t-\t\r\n")
			for _, key := range allKeys {
				asEnvVar := strings.ToUpper(replacer.Replace(key))
				flag := cmd.Parent().Parent().PersistentFlags().Lookup(key)
				if flag == nil {
					continue
				}
				fmt.Fprintf(w, "\t--%s\t%s\t%s\t%s\t\r\n", key, asEnvVar, flag.DefValue, flag.Usage)
			}
		},
	}
	return cmd
}
