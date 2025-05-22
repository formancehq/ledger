package cmd

import (
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func NewDocFlagsCommand() *cobra.Command {
	return &cobra.Command{
		Use: "flags",
		RunE: func(cmd *cobra.Command, _ []string) error {

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 1, ' ', tabwriter.Debug)

			allKeys := make([]string, 0)

			serveCommand := NewServeCommand()
			serveCommand.Flags().VisitAll(func(f *pflag.Flag) {
				allKeys = append(allKeys, f.Name)
			})
			sort.Strings(allKeys)

			if _, err := fmt.Fprintf(w,
				"\tFlag\tEnv var\tDefault value\tDescription\t\r\n"); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w,
				"\t-\t-\t-\t-\t\r\n"); err != nil {
				return err
			}
			for _, key := range allKeys {
				asEnvVar := strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
				flag := serveCommand.Flags().Lookup(key)
				if flag == nil {
					continue
				}
				if _, err := fmt.Fprintf(w,
					"\t --%s\t %s\t %s\t %s\t\r\n", key, asEnvVar, flag.DefValue, flag.Usage); err != nil {
					panic(err)
				}
			}

			return w.Flush()
		},
	}
}
