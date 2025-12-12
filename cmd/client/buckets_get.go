package main

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var getBucketName string

var bucketsGetCmd = &cobra.Command{
	Use:          "get",
	Short:        "Get a bucket",
	Long:         "Retrieves a bucket with its Raft cluster state",
	RunE:         runGetBucket,
	SilenceUsage: true,
}

func init() {
	bucketsGetCmd.Flags().StringVar(&getBucketName, "name", "", "Bucket name to retrieve (required)")
	if err := bucketsGetCmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
	bucketsGetCmd.RegisterFlagCompletionFunc("name", completeBucketNames())
}

func runGetBucket(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if getBucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	sdk := newSDKClient()

	req := operations.GetBucketRequest{
		BucketName: getBucketName,
	}

	res, err := sdk.Buckets.GetBucket(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get bucket: %w", err)
	}

	bucketResponse := res.GetGetBucketResponse()
	if bucketResponse == nil {
		return fmt.Errorf("no bucket data in response")
	}

	data := bucketResponse.Data

	// Bucket information panel
	bucketInfo := ""
	bucketInfo += fmt.Sprintf("ID: %d\n", data.ID)
	bucketInfo += fmt.Sprintf("Name: %s\n", data.Name)
	bucketInfo += fmt.Sprintf("Driver: %s\n", string(data.Driver))
	bucketInfo += fmt.Sprintf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
	if data.SnapshotThreshold != nil && *data.SnapshotThreshold > 0 {
		bucketInfo += fmt.Sprintf("Snapshot Threshold: %d\n", *data.SnapshotThreshold)
	} else {
		bucketInfo += "Snapshot Threshold: (using global config)\n"
	}

	pterm.DefaultHeader.WithFullWidth().Println("Bucket Information")
	pterm.Println()
	pterm.DefaultBox.WithTitle("Bucket Details").WithBoxStyle(pterm.NewStyle(pterm.FgLightCyan)).Println(bucketInfo)

	// Raft state panel
	if data.RaftState != nil {
		pterm.Println()
		raftState := data.RaftState
		raftInfo := ""
		raftInfo += fmt.Sprintf("State: %s\n", raftState.State)
		raftInfo += fmt.Sprintf("Local Node: %d\n", raftState.LocalNode)
		if raftState.Leader != nil && *raftState.Leader != 0 {
			raftInfo += fmt.Sprintf("Leader: %d\n", *raftState.Leader)
		} else {
			raftInfo += "Leader: (none)\n"
		}

		pterm.DefaultBox.WithTitle("Raft Cluster State").WithBoxStyle(pterm.NewStyle(pterm.FgLightMagenta)).Println(raftInfo)

		if len(raftState.Nodes) > 0 {
			pterm.Println()
			tableData := pterm.TableData{
				{"ID", "Address", "Suffrage", "Role"},
			}
			for _, node := range raftState.Nodes {
				nodeID := fmt.Sprintf("%d", node.ID)
				nodeAddr := node.Address
				nodeSuffrage := string(node.Suffrage)
				role := ""
				if raftState.Leader != nil && node.ID == *raftState.Leader {
					role = pterm.LightGreen("LEADER")
				} else {
					role = "Follower"
				}
				tableData = append(tableData, []string{nodeID, nodeAddr, nodeSuffrage, role})
			}
			return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
		}
	} else {
		pterm.Println()
		pterm.Warning.Println("Raft Cluster State: Not available (Raft group not started)")
	}

	return nil
}
