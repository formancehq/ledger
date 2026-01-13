package main

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

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
	if clusterState == nil {
		pterm.Warning.Println("No cluster state data available")
		return nil
	}

	data := clusterState.Data

	// Create cluster info panel
	clusterInfo := ""
	clusterInfo += fmt.Sprintf("Local Node State: %s\n", string(data.State))
	clusterInfo += fmt.Sprintf("Local Node ID: %d\n", data.LocalNode)
	if data.Leader != nil && *data.Leader != 0 {
		clusterInfo += fmt.Sprintf("Leader: %d\n", *data.Leader)
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
			nodeID := fmt.Sprintf("%d", node.ID)
			nodeAddr := node.Address
			nodeSuffrage := string(node.Suffrage)

			// Determine role
			role := "Follower"
			if data.Leader != nil && node.ID == *data.Leader {
				role = pterm.LightGreen("LEADER")
			}
			if node.ID == data.LocalNode {
				if role == "Follower" {
					role = pterm.LightBlue("LOCAL")
				} else {
					role = pterm.LightCyan("LEADER (LOCAL)")
				}
			}

			tableData = append(tableData, []string{nodeID, nodeAddr, nodeSuffrage, role})
		}
		if err := pterm.DefaultTable.WithHasHeader().WithData(tableData).Render(); err != nil {
			return err
		}
	} else {
		pterm.Println()
		pterm.Info.Println("No nodes found")
	}

	// Display Raft Status
	if data.RaftStatus != nil {
		pterm.Println()
		pterm.DefaultHeader.WithFullWidth().Println("Raft Status")
		raftInfo := ""
		raftInfo += fmt.Sprintf("Term: %d\n", data.RaftStatus.Term)
		raftInfo += fmt.Sprintf("Applied: %d\n", data.RaftStatus.Applied)
		raftInfo += fmt.Sprintf("Commit: %d\n", data.RaftStatus.Commit)
		raftInfo += fmt.Sprintf("Last Index: %d\n", data.RaftStatus.LastIndex)
		if data.RaftStatus.Vote != 0 {
			raftInfo += fmt.Sprintf("Vote: %d\n", data.RaftStatus.Vote)
		}

		pterm.DefaultBox.WithTitle("Raft Status").WithBoxStyle(pterm.NewStyle(pterm.FgLightMagenta)).Println(raftInfo)

		// Progress table
		if len(data.RaftStatus.Progress) > 0 {
			pterm.Println()
			progressTableData := pterm.TableData{
				{"Node ID", "Match", "Next", "State", "Pending Snapshot", "Recent Active", "Paused"},
			}
			for nodeID, prog := range data.RaftStatus.Progress {
				progressTableData = append(progressTableData, []string{
					nodeID,
					fmt.Sprintf("%d", prog.Match),
					fmt.Sprintf("%d", prog.Next),
					string(prog.State),
					fmt.Sprintf("%d", prog.PendingSnapshot),
					fmt.Sprintf("%v", prog.RecentActive),
					fmt.Sprintf("%v", prog.IsPaused),
				})
			}
			if err := pterm.DefaultTable.WithHasHeader().WithData(progressTableData).Render(); err != nil {
				return err
			}
		}
	}

	// Display FSM state (SystemState)
	innerState := data.GetInnerState()
	if innerState.GetLedgers() != nil && len(innerState.GetLedgers()) > 0 {
		pterm.Println()
		pterm.DefaultHeader.WithFullWidth().Println("FSM State")
		fsmInfo := ""
		fsmInfo += fmt.Sprintf("Number of Ledgers: %d\n", len(innerState.GetLedgers()))

		pterm.DefaultBox.WithTitle("FSM Information").WithBoxStyle(pterm.NewStyle(pterm.FgLightCyan)).Println(fsmInfo)

		// Ledgers table
		pterm.Println()
		ledgerTableData := pterm.TableData{
			{"Name", "Next Log ID", "Next TX ID", "Last Applied Log ID"},
		}
		for name, ledgerState := range innerState.GetLedgers() {
			ledgerTableData = append(ledgerTableData, []string{
				name,
				fmt.Sprintf("%d", ledgerState.NextLogID),
				fmt.Sprintf("%d", ledgerState.NextTransactionID),
				fmt.Sprintf("%d", ledgerState.LastAppliedLogID),
			})
		}
		return pterm.DefaultTable.WithHasHeader().WithData(ledgerTableData).Render()
	} else {
		pterm.Println()
		pterm.Info.Println("No ledgers in FSM state")
	}

	return nil
}
