package cluster

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// NewStatusCommand creates the cluster status command.
func NewStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "status",
		Aliases:           []string{"st"},
		Short:             "Get cluster status",
		Long:              "Display the current state of the Raft cluster",
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runStatus,
	}

	cmdutil.AddOutputFlags(cmd)
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

	if handled, err := cmdutil.EncodeStructured(cmd, state); handled || err != nil {
		return err
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
	if state.GetMaintenanceMode() {
		maintenanceDisplay = pterm.Red("On")
	}

	syncStatusDisplay := pterm.Green("Normal")
	if sp := state.GetSyncProgress(); sp != nil {
		syncStatusDisplay = getSyncStatusColor(sp.GetStatus())
	}

	overviewData := [][]string{
		{pterm.LightCyan("State:"), getStateColor(state.GetState())},
		{pterm.LightCyan("Local Node ID:"), strconv.FormatUint(uint64(state.GetLocalNode()), 10)},
		{pterm.LightCyan("Leader ID:"), getLeaderDisplay(state.GetLeader())},
		{pterm.LightCyan("Total Nodes:"), strconv.Itoa(len(state.GetNodes()))},
		{pterm.LightCyan("Maintenance:"), maintenanceDisplay},
		{pterm.LightCyan("Sync Status:"), syncStatusDisplay},
	}

	tableStr, _ := pterm.DefaultTable.WithHasHeader(false).WithData(overviewData).Srender()
	b.WriteString(tableStr)
	b.WriteString("\n\n")

	// Show checkpoint fetch progress when syncing
	if sp := state.GetSyncProgress(); sp != nil && sp.GetStatus() == "syncing" && sp.GetBytesTotal() > 0 {
		sectionStr = pterm.DefaultSection.Sprintln("Checkpoint Fetch Progress")
		b.WriteString(sectionStr)
		b.WriteString(renderSyncProgress(sp))
		b.WriteString("\n\n")
	}

	// Raft status
	if state.GetRaftStatus() != nil {
		sectionStr = pterm.DefaultSection.Sprintln("Raft Status")
		b.WriteString(sectionStr)

		raftData := [][]string{
			{pterm.LightCyan("Term:"), strconv.FormatUint(state.GetRaftStatus().GetTerm(), 10)},
			{pterm.LightCyan("Applied Index:"), strconv.FormatUint(state.GetRaftStatus().GetApplied(), 10)},
			{pterm.LightCyan("Commit Index:"), strconv.FormatUint(state.GetRaftStatus().GetCommit(), 10)},
			{pterm.LightCyan("Last Index:"), strconv.FormatUint(state.GetRaftStatus().GetLastIndex(), 10)},
		}

		if state.GetRaftStatus().GetVote() != 0 {
			raftData = append(raftData, []string{pterm.LightCyan("Vote:"), strconv.FormatUint(state.GetRaftStatus().GetVote(), 10)})
		}

		tableStr, _ = pterm.DefaultTable.WithHasHeader(false).WithData(raftData).Srender()
		b.WriteString(tableStr)
		b.WriteString("\n\n")
	}

	// Nodes table - only display if nodes list is available (i.e., querying the leader)
	if len(state.GetNodes()) > 0 {
		sectionStr = pterm.DefaultSection.Sprintln("Cluster Nodes")
		b.WriteString(sectionStr)

		nodeData := [][]string{
			{"ID", "SUFFRAGE", "STATUS", "MATCH", "NEXT", "STATE", "REPLICATION", "SYNC", "INDEX"},
		}

		// Sort nodes by ID for consistent display
		sortedNodes := make([]*clusterpb.NodeInfo, len(state.GetNodes()))
		copy(sortedNodes, state.GetNodes())
		sort.Slice(sortedNodes, func(i, j int) bool {
			return sortedNodes[i].GetId() < sortedNodes[j].GetId()
		})

		commitIndex := uint64(0)
		if state.GetRaftStatus() != nil {
			commitIndex = state.GetRaftStatus().GetCommit()
		}

		for _, node := range sortedNodes {
			status := ""

			switch node.GetId() {
			case state.GetLeader():
				status = pterm.Green("Leader")
			case state.GetLocalNode():
				status = pterm.Yellow("Local")
			}

			matchStr := "-"
			nextStr := "-"
			stateStr := "-"
			progressStr := pterm.Gray("-")

			if node.GetProgress() != nil {
				matchStr = strconv.FormatUint(node.GetProgress().GetMatch(), 10)
				nextStr = strconv.FormatUint(node.GetProgress().GetNext(), 10)
				stateStr = node.GetProgress().GetState()
				isLeader := node.GetId() == state.GetLeader()
				progressStr = formatNodeProgress(node.GetProgress(), commitIndex, isLeader)
			}

			syncStr := pterm.Green("ok")
			if sp := node.GetSyncProgress(); sp != nil {
				syncStr = formatSyncStatus(sp)
			} else if node.GetProgress() != nil && node.GetProgress().GetState() == "Probe" {
				syncStr = pterm.Gray("unknown")
			}

			indexStr := formatIndexProgress(node.GetIndexProgress())

			nodeData = append(nodeData, []string{
				strconv.FormatUint(uint64(node.GetId()), 10),
				node.GetSuffrage(),
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
	if prog.GetState() == "Snapshot" {
		return pterm.Cyan("receiving snapshot...")
	}

	if !prog.GetRecentActive() && !isLeader {
		return pterm.Red("inactive")
	}

	if commitIndex == 0 {
		return pterm.Gray("n/a")
	}

	pct := float64(prog.GetMatch()) / float64(commitIndex) * 100
	if pct > 100 {
		pct = 100
	}

	var lagStr string

	if prog.GetMatch() < commitIndex {
		lag := commitIndex - prog.GetMatch()
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
	switch sp.GetStatus() {
	case "normal":
		return pterm.Green("ok")
	case "syncing":
		if sp.GetBytesTotal() > 0 {
			pct := float64(sp.GetBytesReceived()) / float64(sp.GetBytesTotal()) * 100
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
		return sp.GetStatus()
	}
}

// renderSyncProgress builds the sync progress display as a string.
func renderSyncProgress(sp *clusterpb.SyncProgress) string {
	pct := float64(0)
	if sp.GetBytesTotal() > 0 {
		pct = float64(sp.GetBytesReceived()) / float64(sp.GetBytesTotal()) * 100
		if pct > 100 {
			pct = 100
		}
	}

	receivedMB := float64(sp.GetBytesReceived()) / (1024 * 1024)
	totalMB := float64(sp.GetBytesTotal()) / (1024 * 1024)

	progressData := [][]string{
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

	return pterm.Green(strconv.FormatUint(uint64(leaderID), 10))
}

// formatIndexProgress formats index builder progress for the node table.
func formatIndexProgress(ip *clusterpb.IndexProgress) string {
	if ip == nil {
		return pterm.Gray("unknown")
	}

	if ip.GetPebbleLastSequence() == 0 {
		return pterm.Green("ok")
	}

	lag := int64(ip.GetPebbleLastSequence()) - int64(ip.GetLastIndexedSequence())
	if lag <= 0 {
		return pterm.Green("ok")
	}

	pctBehind := float64(lag) / float64(ip.GetPebbleLastSequence()) * 100
	label := fmt.Sprintf("%s behind (%.1f%%)", formatNumber(uint64(lag)), pctBehind)

	if lag < 1000 {
		return pterm.Yellow(label)
	}

	return pterm.Red(label)
}

// formatNumber formats an integer with comma separators (e.g. 70000000 -> "70,000,000").
func formatNumber(n uint64) string {
	s := strconv.FormatUint(n, 10)
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
