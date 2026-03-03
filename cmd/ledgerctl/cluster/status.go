package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"
)

// NewStatusCommand creates the cluster status command.
func NewStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"st"},
		Short:   "Get cluster status",
		Long:    "Display the current state of the Raft cluster",
		RunE:    runStatus,
	}

	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
	cmd.Flags().Uint32("node-id", 0, "Query specific node by ID (0 = route to leader)")

	return cmd
}

func runStatus(cmd *cobra.Command, _ []string) error {
	// Get gRPC connection
	client, conn, err := cmdutil.GetClusterClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// Get context
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	// Get node-id flag
	nodeID, _ := cmd.Flags().GetUint32("node-id")

	// Get cluster state
	state, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return cmdutil.FormatGRPCError("failed to get cluster state", err)
	}

	jsonOutput, _ := cmd.Flags().GetBool("json")
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(state)
	}

	// Display cluster status
	displayClusterStatus(state)

	return nil
}

func displayClusterStatus(state *clusterpb.ClusterState) {
	pterm.Print(renderClusterStatus(state, true))
}

// renderClusterStatus builds the full cluster status display as a string.
// When showBanner is true, the large "CLUSTER" banner is included.
func renderClusterStatus(state *clusterpb.ClusterState, showBanner bool) string {
	var b strings.Builder

	if showBanner {
		banner, _ := pterm.DefaultBigText.WithLetters(
			putils.LettersFromStringWithStyle("CLUSTER", pterm.FgCyan.ToStyle()),
		).Srender()
		b.WriteString(banner)
		b.WriteString("\n")
	}

	// Cluster overview
	sectionStr := pterm.DefaultSection.Sprintln("Cluster Overview")
	b.WriteString(sectionStr)

	maintenanceDisplay := pterm.Green("Off")
	if state.MaintenanceMode {
		maintenanceDisplay = pterm.Red("On")
	}

	syncStatusDisplay := pterm.Green("Normal")
	if sp := state.SyncProgress; sp != nil {
		syncStatusDisplay = getSyncStatusColor(sp.Status)
	}

	overviewData := [][]string{
		{pterm.LightCyan("State:"), getStateColor(state.State)},
		{pterm.LightCyan("Local Node ID:"), fmt.Sprintf("%d", state.LocalNode)},
		{pterm.LightCyan("Leader ID:"), getLeaderDisplay(state.Leader)},
		{pterm.LightCyan("Total Nodes:"), fmt.Sprintf("%d", len(state.Nodes))},
		{pterm.LightCyan("Maintenance:"), maintenanceDisplay},
		{pterm.LightCyan("Sync Status:"), syncStatusDisplay},
	}

	tableStr, _ := pterm.DefaultTable.WithHasHeader(false).WithData(overviewData).Srender()
	b.WriteString(tableStr)
	b.WriteString("\n\n")

	// Show checkpoint fetch progress when syncing
	if sp := state.SyncProgress; sp != nil && sp.Status == "syncing" && sp.BytesTotal > 0 {
		sectionStr = pterm.DefaultSection.Sprintln("Checkpoint Fetch Progress")
		b.WriteString(sectionStr)
		b.WriteString(renderSyncProgress(sp))
		b.WriteString("\n\n")
	}

	// Raft status
	if state.RaftStatus != nil {
		sectionStr = pterm.DefaultSection.Sprintln("Raft Status")
		b.WriteString(sectionStr)

		raftData := [][]string{
			{pterm.LightCyan("Term:"), fmt.Sprintf("%d", state.RaftStatus.Term)},
			{pterm.LightCyan("Applied Index:"), fmt.Sprintf("%d", state.RaftStatus.Applied)},
			{pterm.LightCyan("Commit Index:"), fmt.Sprintf("%d", state.RaftStatus.Commit)},
			{pterm.LightCyan("Last Index:"), fmt.Sprintf("%d", state.RaftStatus.LastIndex)},
		}

		if state.RaftStatus.Vote != 0 {
			raftData = append(raftData, []string{pterm.LightCyan("Vote:"), fmt.Sprintf("%d", state.RaftStatus.Vote)})
		}

		tableStr, _ = pterm.DefaultTable.WithHasHeader(false).WithData(raftData).Srender()
		b.WriteString(tableStr)
		b.WriteString("\n\n")
	}

	// Nodes table - only display if nodes list is available (i.e., querying the leader)
	if len(state.Nodes) > 0 {
		sectionStr = pterm.DefaultSection.Sprintln("Cluster Nodes")
		b.WriteString(sectionStr)

		nodeData := [][]string{
			{"ID", "SUFFRAGE", "STATUS", "MATCH", "NEXT", "STATE", "REPLICATION", "SYNC", "INDEX"},
		}

		// Sort nodes by ID for consistent display
		sortedNodes := make([]*clusterpb.NodeInfo, len(state.Nodes))
		copy(sortedNodes, state.Nodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			return sortedNodes[i].Id < sortedNodes[j].Id
		})

		commitIndex := uint64(0)
		if state.RaftStatus != nil {
			commitIndex = state.RaftStatus.Commit
		}

		for _, node := range sortedNodes {
			status := ""
			switch node.Id {
			case state.Leader:
				status = pterm.Green("Leader")
			case state.LocalNode:
				status = pterm.Yellow("Local")
			}

			matchStr := "-"
			nextStr := "-"
			stateStr := "-"
			progressStr := pterm.Gray("-")
			if node.Progress != nil {
				matchStr = fmt.Sprintf("%d", node.Progress.Match)
				nextStr = fmt.Sprintf("%d", node.Progress.Next)
				stateStr = node.Progress.State
				isLeader := node.Id == state.Leader
				progressStr = formatNodeProgress(node.Progress, commitIndex, isLeader)
			}

			syncStr := pterm.Green("ok")
			if sp := node.SyncProgress; sp != nil {
				syncStr = formatSyncStatus(sp)
			} else if node.Progress != nil && node.Progress.State == "Probe" {
				syncStr = pterm.Gray("unknown")
			}

			indexStr := formatIndexProgress(node.IndexProgress)

			nodeData = append(nodeData, []string{
				fmt.Sprintf("%d", node.Id),
				node.Suffrage,
				status,
				matchStr,
				nextStr,
				stateStr,
				progressStr,
				syncStr,
				indexStr,
			})
		}

		tableStr, _ = pterm.DefaultTable.WithHasHeader(true).WithData(nodeData).Srender()
		b.WriteString(tableStr)
		b.WriteString("\n\n")
	}

	return b.String()
}

// formatNodeProgress returns a visual progress indicator for a cluster node.
func formatNodeProgress(prog *clusterpb.ProgressInfo, commitIndex uint64, isLeader bool) string {
	if prog.State == "Snapshot" {
		return pterm.Cyan("receiving snapshot...")
	}
	if !prog.RecentActive && !isLeader {
		return pterm.Red("inactive")
	}
	if commitIndex == 0 {
		return pterm.Gray("n/a")
	}

	pct := float64(prog.Match) / float64(commitIndex) * 100
	if pct > 100 {
		pct = 100
	}

	var lagStr string
	if prog.Match < commitIndex {
		lag := commitIndex - prog.Match
		lagStr = fmt.Sprintf(" (%d behind)", lag)
	}

	label := fmt.Sprintf("%.1f%%", pct)
	switch {
	case pct >= 100:
		return pterm.Green(label) + lagStr
	case pct >= 80:
		return pterm.Yellow(label) + pterm.Gray(lagStr)
	default:
		return pterm.Red(label) + pterm.Gray(lagStr)
	}
}

func getSyncStatusColor(status string) string {
	switch status {
	case "normal":
		return pterm.Green("Normal")
	case "syncing":
		return pterm.Cyan("Syncing")
	case "snapshotting":
		return pterm.Yellow("Snapshotting")
	case "out_of_sync":
		return pterm.Red("Out of Sync")
	default:
		return status
	}
}

func formatSyncStatus(sp *clusterpb.SyncProgress) string {
	switch sp.Status {
	case "normal":
		return pterm.Green("ok")
	case "syncing":
		if sp.BytesTotal > 0 {
			pct := float64(sp.BytesReceived) / float64(sp.BytesTotal) * 100
			if pct > 100 {
				pct = 100
			}
			return pterm.Cyan(fmt.Sprintf("%.1f%%", pct))
		}
		return pterm.Cyan("syncing")
	case "snapshotting":
		return pterm.Yellow("snapshotting")
	case "out_of_sync":
		return pterm.Red("out of sync")
	default:
		return sp.Status
	}
}

// renderSyncProgress builds the sync progress display as a string.
func renderSyncProgress(sp *clusterpb.SyncProgress) string {
	pct := float64(0)
	if sp.BytesTotal > 0 {
		pct = float64(sp.BytesReceived) / float64(sp.BytesTotal) * 100
		if pct > 100 {
			pct = 100
		}
	}

	receivedMB := float64(sp.BytesReceived) / (1024 * 1024)
	totalMB := float64(sp.BytesTotal) / (1024 * 1024)

	progressData := [][]string{
		{pterm.LightCyan("Checkpoint ID:"), fmt.Sprintf("%d", sp.CheckpointId)},
		{pterm.LightCyan("Progress:"), pterm.Cyan(fmt.Sprintf("%.1f%%", pct))},
		{pterm.LightCyan("Transferred:"), fmt.Sprintf("%.1f / %.1f MB", receivedMB, totalMB)},
	}

	tableStr, _ := pterm.DefaultTable.WithHasHeader(false).WithData(progressData).Srender()
	return tableStr
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

// formatIndexProgress formats index builder progress for the node table.
func formatIndexProgress(ip *clusterpb.IndexProgress) string {
	if ip == nil {
		return pterm.Gray("unknown")
	}

	if ip.PebbleLastSequence == 0 {
		return pterm.Green("ok")
	}

	lag := int64(ip.PebbleLastSequence) - int64(ip.LastIndexedSequence)
	if lag <= 0 {
		return pterm.Green("ok")
	}

	pctBehind := float64(lag) / float64(ip.PebbleLastSequence) * 100
	label := fmt.Sprintf("%s behind (%.1f%%)", formatNumber(uint64(lag)), pctBehind)

	if lag < 1000 {
		return pterm.Yellow(label)
	}
	return pterm.Red(label)
}

// formatNumber formats an integer with comma separators (e.g. 70000000 -> "70,000,000").
func formatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
