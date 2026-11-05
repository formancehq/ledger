package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// The proof is opaque to restore: admission authenticates the HTTP request;
// backup must preserve those exact bytes and the audit hash binding them.
func TestBackup_HTTPMessageSignatureProofRestoreParity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	source, sourceStore, _ := newSigningParityMachine(t)

	// A historical entry with no HTTP proof must survive both extension of
	// the hash chain and restoration without being rewritten.
	applySigningEntry(t, source, sourceStore, 1, signingRegisterOrder("before", make([]byte, 32), ""))
	historicalKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build()
	historicalBytes, err := dal.GetValue(sourceStore, historicalKey)
	require.NoError(t, err)
	require.Nil(t, lastAuditForKeyTest(t, sourceStore).GetHttpSignature())
	require.NoError(t, sourceStore.Flush())
	_, err = RunBackup(ctx, testLogger(), sourceStore, storage, "http-proof", "full")
	require.NoError(t, err)

	proof := &signaturepb.SignedHTTPRequest{
		Method: "POST", RequestTarget: "/ledger/transactions?preview=false", Authority: "ledger.example.com",
		ContentType: "application/json", ContentDigest: "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
		IdempotencyKey: "signed-after-checkpoint",
		SignatureInput: `sig1=("@method" "@request-target" "@authority" "content-type" "content-digest" "idempotency-key");created=1700000000;keyid="ops";alg="ed25519"`,
		Signature:      "sig1=:AQID:", Body: []byte("{\n  \"postings\": []\n}\n"),
	}
	proposal := &raftcmdpb.Proposal{
		Id: 2, Orders: []*raftcmdpb.Order{signingRegisterOrder("after", make([]byte, 32), "")},
		Date: &commonpb.Timestamp{Data: 1700000002}, ExecutionPlan: &raftcmdpb.ExecutionPlan{},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterConfig), HttpSignature: proof,
	}
	data, err := proto.Marshal(proposal)
	require.NoError(t, err)
	result, err := source.ApplyEntries(ctx, sourceStore, &raftpb.Entry{
		Index: new(uint64(2)), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data,
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)
	require.True(t, result.Results[0].AuditEntryWritten)
	sourceAudit := lastAuditForKeyTest(t, sourceStore)
	require.True(t, proto.Equal(proof, sourceAudit.GetHttpSignature()))
	require.NoError(t, sourceStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, "http-proof", 0)
	require.NoError(t, err)
	require.Positive(t, inc.AuditEntriesExported)
	require.Positive(t, inc.LogEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("http-proof"))
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Exports)

	// This helper downloads the actual checkpoint and calls
	// ApplyExportsAndRebuild, rather than recreating checkpoint state in memory.
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	restoredAudit := lastAuditForKeyTest(t, restored)
	require.True(t, proto.Equal(sourceAudit, restoredAudit), "restore preserves the proof and its original audit hash")
	require.True(t, proto.Equal(proof, restoredAudit.GetHttpSignature()), "including original body whitespace and signature headers")
	for _, store := range []*dal.Store{sourceStore, restored} {
		got, err := dal.GetValue(store, historicalKey)
		require.NoError(t, err)
		require.Equal(t, historicalBytes, got, "historical audit bytes must not be rewritten")
	}
	proofKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(2).Build()
	sourceBytes, err := dal.GetValue(sourceStore, proofKey)
	require.NoError(t, err)
	restoredBytes, err := dal.GetValue(restored, proofKey)
	require.NoError(t, err)
	require.Equal(t, sourceBytes, restoredBytes, "incremental export preserves raw proof-bearing audit rows")
	require.ElementsMatch(t, signingKeyIDs(t, sourceStore), signingKeyIDs(t, restored))
	auditKeyCheckerClean(t, restored)

	// Recovery must consume the proof-bearing head and extend the same chain
	// while leaving its existing hash and signed evidence untouched.
	destination := recoveredAuditKeyMachine(t, restored)
	applySigningEntry(t, destination, restored, destination.State.LastAppliedIndex+1,
		signingRegisterOrder("next", make([]byte, 32), ""))
	require.Equal(t, sourceAudit.GetSequence()+1, lastAuditForKeyTest(t, restored).GetSequence())
	preserved, err := dal.GetValue(restored, proofKey)
	require.NoError(t, err)
	require.Equal(t, sourceBytes, preserved)
	auditKeyCheckerClean(t, restored)
}
