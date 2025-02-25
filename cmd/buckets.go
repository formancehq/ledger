package cmd

import (
	"github.com/spf13/cobra"
)

func NewBucketsCommand() *cobra.Command {
	ret := &cobra.Command{
		Use:     "buckets",
		Aliases: []string{"storage"},
	}

	ret.AddCommand(NewBucketUpgrade())
	return ret
}
