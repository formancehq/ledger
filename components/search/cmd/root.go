package cmd

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/bombsimon/logrusr/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()
}

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

const (
	debugFlag = "debug"
)

func NewRootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "search",
		Short:             "search",
		DisableAutoGenTag: true,
	}

	version := NewVersion()
	root.AddCommand(version)
	server := NewServer()
	root.AddCommand(server)

	root.Flags().Bool(debugFlag, false, "debug mode")
	err := viper.BindPFlags(root.Flags())
	if err != nil {
		panic(err)
	}

	return root
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		if _, err := fmt.Fprintln(os.Stderr, err); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}
