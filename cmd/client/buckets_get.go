package main

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
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
	bucketsGetCmd.MarkFlagRequired("name")
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
	if bucketResponse == nil || bucketResponse.Data == nil {
		return fmt.Errorf("no bucket data in response")
	}

	data := bucketResponse.Data

	fmt.Println("Bucket Information")
	fmt.Println("==================")
	if data.ID != nil {
		fmt.Printf("ID: %d\n", *data.ID)
	}
	if data.Name != nil {
		fmt.Printf("Name: %s\n", *data.Name)
	}
	if data.Driver != nil {
		fmt.Printf("Driver: %s\n", *data.Driver)
	}
	if data.Config != nil {
		configJSON, _ := json.MarshalIndent(data.Config, "", "  ")
		fmt.Printf("Config:\n%s\n", string(configJSON))
	}
	if data.CreatedAt != nil {
		fmt.Printf("Created At: %s\n", data.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	if data.RaftState != nil {
		fmt.Println("\nRaft Cluster State")
		fmt.Println("==================")
		raftState := data.RaftState
		if raftState.State != nil {
			fmt.Printf("State: %s\n", *raftState.State)
		}
		if raftState.LocalNode != nil {
			fmt.Printf("Local Node: %s\n", *raftState.LocalNode)
		}
		if raftState.Leader != nil && *raftState.Leader != "" {
			fmt.Printf("Leader: %s\n", *raftState.Leader)
		} else {
			fmt.Println("Leader: (none)")
		}
		if len(raftState.Nodes) > 0 {
			fmt.Println("\nNodes:")
			for _, node := range raftState.Nodes {
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
				leaderMark := ""
				if raftState.Leader != nil && node.ID != nil && *raftState.Leader == *node.ID {
					leaderMark = " (leader)"
				}
				fmt.Printf("  - ID: %s, Address: %s, Suffrage: %s%s\n", nodeID, nodeAddr, nodeSuffrage, leaderMark)
			}
		} else {
			fmt.Println("\nNodes: (none)")
		}
	} else {
		fmt.Println("\nRaft Cluster State: Not available (Raft group not started)")
	}

	return nil
}

