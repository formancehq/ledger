package main

import (
	"fmt"

	"github.com/pterm/pterm"
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

	// Show spinner while creating snapshot
	spinner, _ := pterm.DefaultSpinner.Start("Creating snapshot...")

	// Call the snapshot endpoint
	res, err := sdk.Cluster.CreateSnapshot(ctx)
	if err != nil {
		spinner.Fail("Snapshot failed")
		return fmt.Errorf("snapshot failed: %w", err)
	}

	message := "Snapshot created successfully"
	if res.SnapshotResponse != nil && res.SnapshotResponse.Data != nil && res.SnapshotResponse.Data.Message != nil {
		message = *res.SnapshotResponse.Data.Message
	}

	spinner.Success(message)
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
		pterm.Warning.Println("No cluster state data available")
		return nil
	}

	data := clusterState.Data

	// Create cluster info panel
	clusterInfo := ""
	if data.State != nil {
		clusterInfo += fmt.Sprintf("Local Node State: %s\n", *data.State)
	}
	if data.LocalNode != nil {
		clusterInfo += fmt.Sprintf("Local Node ID: %s\n", *data.LocalNode)
	}
	if data.Leader != nil && *data.Leader != "" {
		clusterInfo += fmt.Sprintf("Leader: %s\n", *data.Leader)
	} else {
		clusterInfo += "Leader: (none)\n"
	}

	pterm.DefaultHeader.WithFullWidth().Println("Cluster State")
	pterm.Println()
	pterm.DefaultBox.WithTitle("Cluster Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightYellow)).Println(clusterInfo)

	// Nodes table
	if len(data.Nodes) > 0 {
		pterm.Println()
		tableData := pterm.TableData{
			{"ID", "Address", "Suffrage", "Role"},
		}
		for _, node := range data.Nodes {
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

			// Determine role
			role := "Follower"
			if data.Leader != nil && node.ID != nil && *data.Leader == *node.ID {
				role = pterm.LightGreen("LEADER")
			}
			if data.LocalNode != nil && node.ID != nil && *data.LocalNode == *node.ID {
				if role == "Follower" {
					role = pterm.LightBlue("LOCAL")
				} else {
					role = pterm.LightCyan("LEADER (LOCAL)")
				}
			}

			tableData = append(tableData, []string{nodeID, nodeAddr, nodeSuffrage, role})
		}
		return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
	} else {
		pterm.Println()
		pterm.Info.Println("No nodes found")
	}

	return nil
}
