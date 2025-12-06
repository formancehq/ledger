package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:          "snapshot",
	Short:        "Create a Raft cluster snapshot",
	Long:         "Forces the creation of a Raft cluster snapshot on the leader node",
	RunE:         runSnapshot,
	SilenceUsage: true,
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Call the snapshot endpoint
	res, err := sdk.Cluster.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot failed: %w", err)
	}

	if res.SnapshotResponse != nil && res.SnapshotResponse.Data != nil && res.SnapshotResponse.Data.Message != nil {
		fmt.Println(*res.SnapshotResponse.Data.Message)
	} else {
		fmt.Println("Snapshot created successfully")
	}

	return nil
}

var clusterStateCmd = &cobra.Command{
	Use:          "cluster-state",
	Short:        "Get the current state of the Raft cluster",
	Long:         "Returns the current state of the Raft cluster, including the list of nodes and the current leader",
	RunE:         runClusterState,
	SilenceUsage: true,
}

func runClusterState(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := newSDKClient()

	// Call the cluster state endpoint
	res, err := sdk.Cluster.GetClusterState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %w", err)
	}

	// Extract cluster state data
	clusterState := res.GetClusterStateResponse()
	if clusterState == nil || clusterState.Data == nil {
		fmt.Println("No cluster state data available")
		return nil
	}

	data := clusterState.Data

	// Print cluster state information
	fmt.Println("Cluster State")
	fmt.Println("=============")

	// Local node state
	if data.State != nil {
		fmt.Printf("Local Node State: %s\n", *data.State)
	}

	// Local node ID
	if data.LocalNode != nil {
		fmt.Printf("Local Node ID: %s\n", *data.LocalNode)
	}

	// Leader
	if data.Leader != nil && *data.Leader != "" {
		fmt.Printf("Leader: %s\n", *data.Leader)
	} else {
		fmt.Println("Leader: (none)")
	}

	// Nodes list
	fmt.Println("\nNodes:")
	if len(data.Nodes) == 0 {
		fmt.Println("  (no nodes)")
	} else {
		for i, node := range data.Nodes {
			nodeID := "N/A"
			if node.ID != nil {
				nodeID = *node.ID
			}
			nodeAddr := "N/A"
			if node.Address != nil {
				nodeAddr = *node.Address
			}
			nodeSuffrage := "N/A"
			if node.Suffrage != nil {
				nodeSuffrage = string(*node.Suffrage)
			}

			// Mark leader
			leaderMark := ""
			if data.Leader != nil && node.ID != nil && *data.Leader == *node.ID {
				leaderMark = " (LEADER)"
			}

			// Mark local node
			localMark := ""
			if data.LocalNode != nil && node.ID != nil && *data.LocalNode == *node.ID {
				localMark = " (LOCAL)"
			}

			fmt.Printf("  %d. ID: %s, Address: %s, Suffrage: %s%s%s\n",
				i+1, nodeID, nodeAddr, nodeSuffrage, leaderMark, localMark)
		}
	}

	return nil
}

