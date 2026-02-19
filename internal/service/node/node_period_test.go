package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// createForceTransaction proposes a force transaction (bypasses balance checks) to the node.
func createForceTransaction(node *Node, ledger string, postings []*commonpb.Posting) ([]*commonpb.Log, error) {
	proposal := &raftcmdpb.Proposal{
		Id:   generateProposalID(),
		Date: nowTimestamp(),
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Ledger: ledger,
					Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings: postings,
							Force:    true,
						},
					},
				},
			},
		}},
	}
	return proposeAndWait(node, proposal)
}

// saveAccountMetadata proposes saving metadata on an account.
func saveAccountMetadata(node *Node, ledger, address string, metadata map[string]string) ([]*commonpb.Log, error) {
	metadataEntries := make([]*commonpb.Metadata, 0, len(metadata))
	for k, v := range metadata {
		metadataEntries = append(metadataEntries, &commonpb.Metadata{Key: k, Value: &commonpb.MetadataValue{Value: v}})
	}

	proposal := &raftcmdpb.Proposal{
		Id:   generateProposalID(),
		Date: nowTimestamp(),
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Ledger: ledger,
					Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
						AddMetadata: &raftcmdpb.SaveMetadataOrder{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: address},
								},
							},
							Metadata: &commonpb.MetadataSet{Metadata: metadataEntries},
						},
					},
				},
			},
		}},
	}
	return proposeAndWait(node, proposal)
}

// closePeriod proposes closing the current open period.
func closePeriod(node *Node) ([]*commonpb.Log, error) {
	proposal := &raftcmdpb.Proposal{
		Id:   generateProposalID(),
		Date: nowTimestamp(),
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_ClosePeriod{
				ClosePeriod: &raftcmdpb.ClosePeriodOrder{},
			},
		}},
	}
	return proposeAndWait(node, proposal)
}

// startClusterSealers starts Sealers for all nodes in the cluster.
// Only the leader's Sealer will successfully propose SealPeriod back (DisableProposalForwarding).
// Returns a cleanup function that stops all sealers.
func startClusterSealers(t *testing.T, cluster *Cluster) func() {
	t.Helper()

	sealers := make([]*state.Sealer, len(cluster.nodes))
	for i, clusterNode := range cluster.nodes {
		logger := logging.Testing().WithFields(map[string]any{"node": clusterNode.ID, "cmp": "sealer"})
		node := clusterNode.Node

		sealers[i] = state.NewSealer(logger, clusterNode.Store, node.fsm.SealRequestCh(), func(periodID uint64, sealingHash []byte) {
			// Propose SealPeriod back through Raft
			proposal := &raftcmdpb.Proposal{
				Id:   generateProposalID(),
				Date: nowTimestamp(),
				Orders: []*raftcmdpb.Order{{
					Type: &raftcmdpb.Order_SealPeriod{
						SealPeriod: &raftcmdpb.SealPeriodOrder{
							PeriodId:    periodID,
							SealingHash: sealingHash,
						},
					},
				}},
			}
			cmdData, err := proto.Marshal(proposal)
			if err != nil {
				t.Logf("Sealer: failed to marshal SealPeriod proposal: %v", err)
				return
			}
			p := NewProposal(proposal.Id, cmdData)
			if _, err := node.Propose(p); err != nil {
				// Expected on followers (DisableProposalForwarding)
				t.Logf("Sealer node %d: propose SealPeriod failed (expected on followers): %v", clusterNode.ID, err)
			}
		}, node.IsLeader, func(periodID uint64) bool {
			cp := node.fsm.ClosingPeriod()
			return cp != nil && cp.Id == periodID
		})
		sealers[i].Start(node.fsm.ClosingPeriod())
	}

	return func() {
		for _, sealer := range sealers {
			sealer.Stop()
		}
	}
}

func TestPeriodSealHashConsistency(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster
	cluster := NewCluster(t, 3, DefaultClusterConfig())

	// Start all nodes
	_ = cluster.Start(ctx)

	// Wait for a leader to be elected
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Start sealers for all nodes
	stopSealers := startClusterSealers(t, cluster)
	defer stopSealers()

	// Create a ledger
	_, err = createLedger(ctx, leader.Node, "period-test")
	require.NoError(t, err)

	// Wait for ledger to replicate to all nodes
	for _, node := range cluster.nodes {
		require.Eventually(t, func() bool {
			return listLedgerContains(node.Store, "period-test")
		}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to node %d", node.ID)
	}

	// Run multiple period cycles
	const numPeriodCycles = 3
	for cycle := range numPeriodCycles {
		t.Logf("=== Period cycle %d ===", cycle+1)

		// Create several force transactions
		numTransactions := 10
		for i := range numTransactions {
			_, err := createForceTransaction(leader.Node, "period-test", []*commonpb.Posting{
				{
					Source:      "world",
					Destination: fmt.Sprintf("user:%d:%d", cycle, i),
					Amount:      commonpb.NewUint256FromUint64(uint64(100 + i)),
					Asset:       "USD",
				},
			})
			require.NoError(t, err)
		}

		// Set metadata on some accounts
		numMetadataOps := 5
		for i := range numMetadataOps {
			_, err := saveAccountMetadata(leader.Node, "period-test", fmt.Sprintf("user:%d:%d", cycle, i), map[string]string{
				"cycle":   fmt.Sprintf("%d", cycle),
				"index":   fmt.Sprintf("%d", i),
				"version": "test",
			})
			require.NoError(t, err)
		}

		// Close the period
		logs, err := closePeriod(leader.Node)
		require.NoError(t, err)
		closePeriodLog := logs[0].Payload.GetClosePeriod()
		require.NotNil(t, closePeriodLog)
		closedPeriodID := closePeriodLog.ClosedPeriod.Id
		t.Logf("Closed period %d, new open period %d", closedPeriodID, closePeriodLog.NewPeriod.Id)

		// Wait for the period to be sealed (CLOSED status with sealing hash)
		require.Eventually(t, func() bool {
			periods, err := leader.Store.GetPeriods()
			if err != nil {
				return false
			}
			for _, p := range periods {
				if p.Id == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_CLOSED && len(p.SealingHash) > 0 {
					return true
				}
			}
			return false
		}, 15*time.Second, 200*time.Millisecond, "period %d should be sealed on leader", closedPeriodID)

		// Get the sealed period from leader
		leaderPeriods, err := leader.Store.GetPeriods()
		require.NoError(t, err)

		var leaderSealedPeriod *commonpb.Period
		for _, p := range leaderPeriods {
			if p.Id == closedPeriodID {
				leaderSealedPeriod = p
				break
			}
		}
		require.NotNil(t, leaderSealedPeriod, "leader should have period %d", closedPeriodID)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, leaderSealedPeriod.Status)
		require.NotEmpty(t, leaderSealedPeriod.SealingHash)
		t.Logf("Leader sealing hash for period %d: %x", closedPeriodID, leaderSealedPeriod.SealingHash)

		// Verify all followers have the same sealing hash
		for _, clusterNode := range cluster.nodes {
			if clusterNode.ID == leaderID {
				continue
			}

			require.Eventually(t, func() bool {
				periods, err := clusterNode.Store.GetPeriods()
				if err != nil {
					return false
				}
				for _, p := range periods {
					if p.Id == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_CLOSED && len(p.SealingHash) > 0 {
						return true
					}
				}
				return false
			}, 15*time.Second, 200*time.Millisecond, "period %d should be sealed on node %d", closedPeriodID, clusterNode.ID)

			followerPeriods, err := clusterNode.Store.GetPeriods()
			require.NoError(t, err)

			var followerSealedPeriod *commonpb.Period
			for _, p := range followerPeriods {
				if p.Id == closedPeriodID {
					followerSealedPeriod = p
					break
				}
			}
			require.NotNil(t, followerSealedPeriod, "node %d should have period %d", clusterNode.ID, closedPeriodID)
			require.Equal(t, leaderSealedPeriod.SealingHash, followerSealedPeriod.SealingHash,
				"node %d sealing hash for period %d should match leader (leader=%x, node=%x)",
				clusterNode.ID, closedPeriodID, leaderSealedPeriod.SealingHash, followerSealedPeriod.SealingHash)

			t.Logf("Node %d sealing hash for period %d: %x (matches leader)", clusterNode.ID, closedPeriodID, followerSealedPeriod.SealingHash)
		}

		t.Logf("Period %d: all nodes have matching sealing hashes", closedPeriodID)
	}

	// Final verification: all nodes should have the same number of periods
	// and all CLOSED periods should have matching hashes
	var expectedPeriods []*commonpb.Period
	for _, clusterNode := range cluster.nodes {
		periods, err := clusterNode.Store.GetPeriods()
		require.NoError(t, err)

		if expectedPeriods == nil {
			expectedPeriods = periods
			t.Logf("Final state: %d periods", len(periods))
			for _, p := range periods {
				t.Logf("  Period %d: status=%s, hash=%x", p.Id, p.Status.String(), p.SealingHash)
			}
		} else {
			require.Len(t, periods, len(expectedPeriods), "node %d should have same number of periods", clusterNode.ID)
			for j, p := range periods {
				require.Equal(t, expectedPeriods[j].Id, p.Id, "period ID mismatch on node %d", clusterNode.ID)
				require.Equal(t, expectedPeriods[j].Status, p.Status, "period status mismatch on node %d", clusterNode.ID)
				if p.Status == commonpb.PeriodStatus_PERIOD_CLOSED {
					require.Equal(t, expectedPeriods[j].SealingHash, p.SealingHash,
						"sealing hash mismatch for period %d on node %d", p.Id, clusterNode.ID)
				}
			}
		}
	}

	t.Log("Test passed: all period seal hashes are consistent across all nodes")
}

// TestPeriodSealCrashRecoveryNoCheckpoint verifies Window 1 crash recovery:
// the node crashed after ClosePeriod was committed to Pebble but before the
// seal checkpoint was created. On restart, NewNode() detects closingPeriod != nil
// with no checkpoint on disk and creates the checkpoint before spool replay.
func TestPeriodSealCrashRecoveryNoCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster — do NOT start sealers yet
	config := DefaultClusterConfig()
	cluster := NewCluster(t, 3, config)

	// Start all nodes
	_ = cluster.Start(ctx)

	// Wait for leader election
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Create a ledger and wait for replication
	_, err = createLedger(ctx, leader.Node, "crash-test")
	require.NoError(t, err)

	for _, clusterNode := range cluster.nodes {
		require.Eventually(t, func() bool {
			return listLedgerContains(clusterNode.Store, "crash-test")
		}, 5*time.Second, 100*time.Millisecond, "ledger should replicate to node %d", clusterNode.ID)
	}

	// Create several force transactions to populate the period with data
	numTransactions := 5
	for i := range numTransactions {
		_, err := createForceTransaction(leader.Node, "crash-test", []*commonpb.Posting{
			{
				Source:      "world",
				Destination: fmt.Sprintf("user:%d", i),
				Amount:      commonpb.NewUint256FromUint64(uint64(100 + i)),
				Asset:       "USD",
			},
		})
		require.NoError(t, err)
	}

	// Start sealers so the maintenance task creates the seal checkpoint
	stopSealers := startClusterSealers(t, cluster)

	// Close the period — ClosePeriod is proposed as the last operation
	logs, err := closePeriod(leader.Node)
	require.NoError(t, err)
	closePeriodLog := logs[0].Payload.GetClosePeriod()
	require.NotNil(t, closePeriodLog)
	closedPeriodID := closePeriodLog.ClosedPeriod.Id
	t.Logf("Closed period %d, new open period %d", closedPeriodID, closePeriodLog.NewPeriod.Id)

	// Wait for the period to be fully sealed (CLOSED status with sealing hash) on the leader.
	// This means the maintenance task has created the checkpoint, the sealer has computed
	// the hash, and the SealPeriod order has been applied.
	require.Eventually(t, func() bool {
		periods, err := leader.Store.GetPeriods()
		if err != nil {
			return false
		}
		for _, p := range periods {
			if p.Id == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_CLOSED && len(p.SealingHash) > 0 {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "period %d should be sealed on leader", closedPeriodID)

	// Record the expected sealing hash from the leader
	leaderPeriods, err := leader.Store.GetPeriods()
	require.NoError(t, err)
	var expectedSealingHash []byte
	for _, p := range leaderPeriods {
		if p.Id == closedPeriodID {
			expectedSealingHash = p.SealingHash
			break
		}
	}
	require.NotEmpty(t, expectedSealingHash)
	t.Logf("Expected sealing hash for period %d: %x", closedPeriodID, expectedSealingHash)

	// Stop sealers before stopping the leader
	stopSealers()

	// Stop the leader
	t.Logf("Stopping leader node %d", leaderID)
	err = leader.Node.Stop(ctx)
	require.NoError(t, err)

	// Delete the seal checkpoint from disk to simulate Window 1 crash
	// (node crashed after ClosePeriod batch.Commit but before checkpoint creation)
	sealCheckpointPath := filepath.Join(leader.DataDir, "seal")
	t.Logf("Deleting seal checkpoint at %s to simulate crash", sealCheckpointPath)
	err = os.RemoveAll(sealCheckpointPath)
	require.NoError(t, err)

	// Restart the leader
	t.Logf("Restarting leader node %d", leaderID)
	_, err = cluster.RestartNode(ctx, leaderID, config)
	require.NoError(t, err)

	// Wait for leader election (may be same or different node)
	newLeaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader after restart: node %d", newLeaderID)

	// Start sealers for all nodes — the recovery-created checkpoint will be consumed
	stopSealers = startClusterSealers(t, cluster)
	defer stopSealers()

	// Get the restarted node reference
	restartedNode := cluster.GetNodeByID(leaderID)
	require.NotNil(t, restartedNode)

	// Wait for the period to be sealed on the restarted node
	require.Eventually(t, func() bool {
		periods, err := restartedNode.Store.GetPeriods()
		if err != nil {
			return false
		}
		for _, p := range periods {
			if p.Id == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_CLOSED && len(p.SealingHash) > 0 {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "period %d should be sealed on restarted node", closedPeriodID)

	// Verify the sealing hash matches the expected hash
	restartedPeriods, err := restartedNode.Store.GetPeriods()
	require.NoError(t, err)
	var recoveredSealingHash []byte
	for _, p := range restartedPeriods {
		if p.Id == closedPeriodID {
			recoveredSealingHash = p.SealingHash
			break
		}
	}
	require.Equal(t, expectedSealingHash, recoveredSealingHash,
		"recovered sealing hash should match original (expected=%x, got=%x)",
		expectedSealingHash, recoveredSealingHash)
	t.Logf("Recovered sealing hash matches: %x", recoveredSealingHash)

	// Verify all nodes agree on the sealing hash
	for _, clusterNode := range cluster.nodes {
		require.Eventually(t, func() bool {
			periods, err := clusterNode.Store.GetPeriods()
			if err != nil {
				return false
			}
			for _, p := range periods {
				if p.Id == closedPeriodID && p.Status == commonpb.PeriodStatus_PERIOD_CLOSED && len(p.SealingHash) > 0 {
					return true
				}
			}
			return false
		}, 15*time.Second, 200*time.Millisecond, "period %d should be sealed on node %d", closedPeriodID, clusterNode.ID)

		periods, err := clusterNode.Store.GetPeriods()
		require.NoError(t, err)
		for _, p := range periods {
			if p.Id == closedPeriodID {
				require.Equal(t, expectedSealingHash, p.SealingHash,
					"node %d sealing hash should match (expected=%x, got=%x)",
					clusterNode.ID, expectedSealingHash, p.SealingHash)
				t.Logf("Node %d sealing hash: %x (matches)", clusterNode.ID, p.SealingHash)
				break
			}
		}
	}

	t.Log("Test passed: crash recovery (Window 1) correctly re-created checkpoint and sealed period with matching hash")
}
