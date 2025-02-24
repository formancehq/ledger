package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func PrintVersion(_ *cobra.Command, _ []string) {
	fmt.Printf("Version: %s \n", Version)
	fmt.Printf("Date: %s \n", BuildDate)
	fmt.Printf("Commit: %s \n", Commit)
}

func NewVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Get version",
		Run:   PrintVersion,
	}
}
