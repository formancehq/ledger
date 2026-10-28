package node

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/antithesistest"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// Exercise the production apply entry points with a nonzero durable baseline,
// repeated writes to the same volume keys and a higher Raft term. The WAL also
// holds an uncommitted suffix: replay must stop at HardState.Commit even though
// the additional transfer would satisfy every local accounting invariant.
func TestSentinelRaftApplyLifecycle(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"live_commit", "wal_restart", "spool_replay"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			dir := t.TempDir()
			// The SDK emits only the first successful observation per property.
			// Disable bootstrap sentinels so they cannot certify the path below.
			setup, closeSetup := newSentinelApplier(t, dir, false)
			const ledger = "raft-sentinel"
			initial := makeSentinelRaftBootstrap(t, ledger)
			entries := []*raftpb.Entry{initial}
			for index := uint64(2); index <= 6; index++ {
				entry := makeDeleteSentinelEntry(t, index, ledger, false)
				if index >= 3 {
					entry.Term = proto.Uint64(2)
				}
				entries = append(entries, entry)
			}
			require.NoError(t, setup.wal.Append(&raftpb.HardState{
				Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(5),
			}, entries))

			baseline, err := setup.fsm.ApplyEntries(ctx, setup.store, entries[:2]...)
			require.NoError(t, err)
			for _, result := range baseline.Results {
				require.NoError(t, result.Error)
			}
			closeSetup()
			setup, _ = newSentinelApplier(t, dir, true)
			if path != "wal_restart" {
				require.NoError(t, state.NewRecovery(setup.fsm, setup.store).RestoreCacheFromStore())
			}
			require.Equal(t, uint64(2), setup.fsm.State.LastAppliedIndex)

			pending := make([]*futures.Future[state.ApplyResult], 0, 3)
			for _, entry := range entries[2:5] {
				future := futures.New[state.ApplyResult]()
				setup.applier.StoreFuture(entry.GetIndex(), entry.GetTerm(), future)
				pending = append(pending, future)
			}
			// Replaying in batches of two exercises both cross-entry reduction
			// and the next commit's nonzero expected baseline.
			setup.applier.replayBatchSize = 2
			switch path {
			case "live_commit":
				runDeleteSentinelCatchUp(t, ctx, setup, entries[2:5])
			case "wal_restart":
				upToDate, err := setup.applier.RecoverAndReplay(ctx)
				require.NoError(t, err)
				require.True(t, upToDate)
			case "spool_replay":
				require.NoError(t, setup.spool.AppendCommittedEntries(ctx, entries[2:5]...))
				require.NoError(t, setup.applier.unspoolAndResume(ctx))
				require.EqualValues(t, statusNormal, setup.applier.Status())
			}

			for i, future := range pending {
				result, err := future.Wait(ctx)
				require.NoError(t, err)
				require.NoError(t, result.Error, "a rejected business order cannot certify the sentinel")
				require.Equal(t, uint64(i+3), result.AppliedIndex)
				require.Len(t, result.Logs, 1)
				tx := result.Logs[0].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
				require.NotNil(t, tx)
				require.Len(t, tx.GetPostings(), 1)
				require.Equal(t, "100", tx.GetPostings()[0].GetAmount().ToBigInt().String())
			}
			persisted, err := query.ReadLastAppliedIndex(setup.store)
			require.NoError(t, err)
			require.Equal(t, uint64(5), persisted, "the uncommitted WAL suffix must not be applied")
			require.Equal(t, uint64(5), setup.fsm.LastPersistedIndex())
			for _, account := range []string{"world", "treasury"} {
				pair, err := setup.fsm.Registry.Attrs.Volume.Get(setup.store, domain.NewVolumeKey(ledger, account, "EUR", "").Bytes())
				require.NoError(t, err)
				require.NotNil(t, pair)
				input, output := "400", "0"
				if account == "world" {
					input, output = output, input
				}
				require.Equal(t, input, pair.GetInput().ToBigInt().String())
				require.Equal(t, output, pair.GetOutput().ToBigInt().String())
			}
		})
	}
}

func makeSentinelRaftBootstrap(t *testing.T, ledger string) *raftpb.Entry {
	t.Helper()
	entry, _ := makeCreateLedgerEntry(t, 1, ledger)
	var proposal raftcmdpb.Proposal
	require.NoError(t, proposal.UnmarshalVT(entry.GetData()))
	proposal.Orders = append([]*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{
		SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: &commonpb.ClusterPolicy{
				Revision: 1, QueryCheckpointLimit: 10,
				MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
				MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
				MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
				MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
				MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
			}},
		}},
	}}}, proposal.GetOrders()...)
	var err error
	entry.Data, err = proposal.MarshalVT()
	require.NoError(t, err)

	return entry
}

// Healthy volumes alone do not prove SENTINEL_MODE executed. Require its SDK
// coverage event at the first target business commit, after the disabled
// bootstrap. The SDK deduplicates later successful observations of a property.
func TestSentinelRaftAntithesisEmission(t *testing.T) {
	if !antithesistest.Armed {
		t.Skip(antithesistest.UnarmedSkip)
	}
	// Run subprocess probes before parallel WAL fixtures: fork can briefly
	// inherit their file locks between WAL creation's close and reopen.
	for _, path := range []string{"live_commit", "wal_restart", "spool_replay"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			found, err := antithesistest.Emitted(ctx, dir, "TestSentinelRaftApplyLifecycle/"+path,
				"nonempty sentinel verification completed", true)
			require.NoError(t, err)
			require.True(t, found)
			file, err := os.Open(filepath.Join(dir, "assertions.jsonl"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			scanner := bufio.NewScanner(file)
			scanner.Buffer(nil, 1<<20)
			wantIndex := uint64(4)
			if path == "live_commit" {
				wantIndex = 5
			}
			targetCommitVerified := false
			for scanner.Scan() {
				var event struct {
					Assertion *struct {
						Message     string `json:"message"`
						Hit         bool   `json:"hit"`
						Condition   bool   `json:"condition"`
						DisplayType string `json:"display_type"`
						Details     struct {
							RaftIndex     uint64 `json:"raftIndex"`
							VolumeUpdates int    `json:"volumeUpdates"`
						} `json:"details"`
					} `json:"antithesis_assert"`
				}
				require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
				a := event.Assertion
				if a == nil || !a.Hit {
					continue
				}
				require.NotEqual(t, "Unreachable", a.DisplayType, "healthy apply reported %q", a.Message)
				if a.DisplayType == "Always" || a.DisplayType == "AlwaysOrUnreachable" {
					require.True(t, a.Condition, "healthy apply violated %q", a.Message)
				}
				if a.Condition && a.Message == "nonempty sentinel verification completed" {
					require.Equal(t, wantIndex, a.Details.RaftIndex)
					require.Equal(t, 2, a.Details.VolumeUpdates)
					targetCommitVerified = true
				}
			}
			require.NoError(t, scanner.Err())
			require.True(t, targetCommitVerified, "the recovered/applied business batch must execute the nonempty sentinel")
		})
	}
}
