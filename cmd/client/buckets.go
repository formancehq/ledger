package main

import (
	"github.com/spf13/cobra"
)

var bucketsCmd = &cobra.Command{
	Use:          "buckets",
	Short:        "Manage buckets",
	Long:         "Commands for managing buckets in the cluster",
	SilenceUsage: true,
}

func initBuckets() {
	bucketsCmd.AddCommand(bucketsCreateCmd)
	bucketsCmd.AddCommand(bucketsListCmd)
	bucketsCmd.AddCommand(bucketsGetCmd)
	bucketsCmd.AddCommand(bucketsRaftStateCmd)
	bucketsCmd.AddCommand(bucketsDeleteCmd)
	bucketsCmd.AddCommand(bucketsSnapshotCmd)
}
