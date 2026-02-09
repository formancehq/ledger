package main

import (
	"crypto/tls"
	"fmt"
	"sort"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// newClusterStatusCommand creates the cluster status command.
func newClusterStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"st"},
		Short:   "Get cluster status",
		Long:    "Display the current state of the Raft cluster",
		RunE:    runClusterStatus,
	}

	cmd.Flags().Duration("timeout", defaultTimeout, "Request timeout")
	cmd.Flags().Uint32("node-id", 0, "Query specific node by ID (0 = route to leader)")

	return cmd
}

func runClusterStatus(cmd *cobra.Command, args []string) error {
	// Get gRPC connection
	client, conn, err := getClusterClient(cmd)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get context
	ctx, cancel := getContext(cmd)
	defer cancel()

	// Get node-id flag
	nodeID, _ := cmd.Flags().GetUint32("node-id")

	// Get cluster state
	state, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %w", err)
	}

	// Display cluster status
	displayClusterStatus(state)

	return nil
}

// getClusterClient creates a gRPC client connection for cluster operations.
func getClusterClient(cmd *cobra.Command) (clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	serverAddr, _ := cmd.Flags().GetString("server")
	insecureMode, _ := cmd.Flags().GetBool("insecure")

	var creds credentials.TransportCredentials
	if insecureMode {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	return clusterpb.NewClusterServiceClient(conn), conn, nil
}

func displayClusterStatus(state *clusterpb.ClusterState) {
	// Display banner
	banner, _ := pterm.DefaultBigText.WithLetters(
		pterm.NewLettersFromStringWithStyle("CLUSTER", pterm.FgCyan.ToStyle()),
	).Srender()
	pterm.Println(banner)

	// Cluster overview
	pterm.DefaultSection.Println("Cluster Overview")

	overviewData := [][]string{
		{pterm.LightCyan("State:"), getStateColor(state.State)},
		{pterm.LightCyan("Local Node ID:"), fmt.Sprintf("%d", state.LocalNode)},
		{pterm.LightCyan("Leader ID:"), getLeaderDisplay(state.Leader)},
		{pterm.LightCyan("Total Nodes:"), fmt.Sprintf("%d", len(state.Nodes))},
	}

	pterm.DefaultTable.WithHasHeader(false).WithData(overviewData).Render()
	pterm.Println()

	// Raft status
	if state.RaftStatus != nil {
		pterm.DefaultSection.Println("Raft Status")

		raftData := [][]string{
			{pterm.LightCyan("Term:"), fmt.Sprintf("%d", state.RaftStatus.Term)},
			{pterm.LightCyan("Applied Index:"), fmt.Sprintf("%d", state.RaftStatus.Applied)},
			{pterm.LightCyan("Commit Index:"), fmt.Sprintf("%d", state.RaftStatus.Commit)},
			{pterm.LightCyan("Last Index:"), fmt.Sprintf("%d", state.RaftStatus.LastIndex)},
		}

		if state.RaftStatus.Vote != 0 {
			raftData = append(raftData, []string{pterm.LightCyan("Vote:"), fmt.Sprintf("%d", state.RaftStatus.Vote)})
		}

		pterm.DefaultTable.WithHasHeader(false).WithData(raftData).Render()
		pterm.Println()
	}

	// Nodes table - only display if nodes list is available (i.e., querying the leader)
	if len(state.Nodes) > 0 {
		pterm.DefaultSection.Println("Cluster Nodes")

		// Build table header
		nodeData := [][]string{
			{
				pterm.Bold.Sprint("ID"),
				pterm.Bold.Sprint("Suffrage"),
				pterm.Bold.Sprint("Status"),
				pterm.Bold.Sprint("Match"),
				pterm.Bold.Sprint("Next"),
				pterm.Bold.Sprint("State"),
				pterm.Bold.Sprint("Active"),
			},
		}

		// Sort nodes by ID for consistent display
		sortedNodes := make([]*clusterpb.NodeInfo, len(state.Nodes))
		copy(sortedNodes, state.Nodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			return sortedNodes[i].Id < sortedNodes[j].Id
		})

		for _, node := range sortedNodes {
			status := ""
			switch node.Id {
			case state.Leader:
				status = pterm.Green("Leader")
			case state.LocalNode:
				status = pterm.Yellow("Local")
			}

			// Include progress information (should always be present when nodes list is available)
			active := pterm.Red("No")
			if node.Progress != nil && node.Progress.RecentActive {
				active = pterm.Green("Yes")
			}

			matchStr := "-"
			nextStr := "-"
			stateStr := "-"
			if node.Progress != nil {
				matchStr = fmt.Sprintf("%d", node.Progress.Match)
				nextStr = fmt.Sprintf("%d", node.Progress.Next)
				stateStr = node.Progress.State
			}

			nodeData = append(nodeData, []string{
				fmt.Sprintf("%d", node.Id),
				node.Suffrage,
				status,
				matchStr,
				nextStr,
				stateStr,
				active,
			})
		}

		pterm.DefaultTable.WithHasHeader(true).WithData(nodeData).Render()
		pterm.Println()
	}
}

func getStateColor(state string) string {
	switch state {
	case "Leader":
		return pterm.Green(state)
	case "Follower":
		return pterm.Yellow(state)
	case "Candidate", "PreCandidate":
		return pterm.LightYellow(state)
	case "Shutdown":
		return pterm.Red(state)
	default:
		return state
	}
}

func getLeaderDisplay(leaderID uint32) string {
	if leaderID == 0 {
		return pterm.Red("No leader")
	}
	return pterm.Green(fmt.Sprintf("%d", leaderID))
}
