package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func marshalSigningOrder(t *testing.T, order *raftcmdpb.Order) []byte {
	t.Helper()

	serialized, err := order.MarshalVT()
	require.NoError(t, err)

	return serialized
}

// writeSigningAuditEntry persists one success entry whose fresh log range is
// [minLog, maxLog], chained onto prevHash, and returns its own hash.
//
// persistAuditEntry cannot be reused: it always computes with a nil previous
// hash, which is correct for a single-entry fixture but breaks the chain from the
// second entry on. A broken chain matters here because a mismatching entry is not
// folded, so the revoke would silently never apply and the test would pass for
// the wrong reason.
func writeSigningAuditEntry(
	t *testing.T,
	store *dal.Store,
	clusterID string,
	prevHash []byte,
	seq, minLog, maxLog uint64,
	items []*auditpb.AuditItem,
) []byte {
	t.Helper()

	entry := &auditpb.AuditEntry{
		Sequence:    seq,
		Timestamp:   &commonpb.Timestamp{Data: 1700000000 + seq},
		OrderCount:  uint32(len(items)),
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{MinLogSequence: minLog, MaxLogSequence: maxLog},
		},
	}

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)
	_, entry.Hash = gen.Compute(nil, prevHash, hashSlices)

	rewriteAuditEntry(t, store, entry, items)

	return entry.GetHash()
}

// TestVerifyAuditHashChain_SigningFoldIgnoresLegacyReplayReferences pins the
// fresh-log window on the live signing fold.
//
// On stores upgraded from before state commit f9ee1e829, an idempotent replay
// still wrote an audit entry and buildAuditItems persisted the REFERENCED log
// sequence into AuditItem.LogSequence. Such an item points at a log an earlier
// entry already folded, so folding it again replays that order out of order.
//
// The individual orders are idempotent, which is why this is easy to get wrong:
// what is not idempotent is the ORDERING. Register at log 1, revoke at log 2,
// then a reference item pointing back at log 1 re-registers the key after its
// revocation, leaving it in the expected set and producing a false
// SIGNING_KEY_MISMATCH against a perfectly healthy store.
//
// AuditSuccess.{Min,Max}LogSequence are computed only over freshly-created logs,
// so the window admits this entry's own items and excludes the back-reference —
// the same discriminant collectExpectedSkippable uses for the nextTxID fold.
func TestVerifyAuditHashChain_SigningFoldIgnoresLegacyReplayReferences(t *testing.T) {
	t.Parallel()

	const clusterID = "signing-fold-window-cluster"

	store := createTestStore(t)
	publicKey := signingTestKey(0x33)

	hash := writeSigningAuditEntry(t, store, clusterID, nil, 1, 1, 1, []*auditpb.AuditItem{{
		OrderIndex:      0,
		LogSequence:     1,
		SerializedOrder: marshalSigningOrder(t, registerSigningKeyOrder("legacy-key", publicKey, "")),
	}})

	hash = writeSigningAuditEntry(t, store, clusterID, hash, 2, 2, 2, []*auditpb.AuditItem{{
		OrderIndex:      0,
		LogSequence:     2,
		SerializedOrder: marshalSigningOrder(t, revokeSigningKeyOrder("legacy-key", false)),
	}})

	// The upgraded-store shape: one fresh item of this entry (log 3) alongside a
	// legacy per-order replay reference pointing back at log 1. Both are carried
	// by the same successful entry, so only the window separates them.
	writeSigningAuditEntry(t, store, clusterID, hash, 3, 3, 3, []*auditpb.AuditItem{
		{
			OrderIndex:      0,
			LogSequence:     3,
			SerializedOrder: marshalSigningOrder(t, setSigningConfigOrder(true)),
		},
		{
			OrderIndex:      1,
			LogSequence:     1,
			SerializedOrder: marshalSigningOrder(t, registerSigningKeyOrder("legacy-key", publicKey, "")),
		},
	})

	verifier := newSigningVerifier()
	checker := NewChecker(store, attributes.New(), clusterID, nil, nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	// Events are discarded on purpose: the assertions read the folded expectation
	// directly, since that is the state a false mismatch would be derived from. The
	// chain itself must still be intact — a mismatching entry is not folded, so a
	// broken chain would skip the revoke and pass for the wrong reason.
	_, err = checker.verifyAuditHashChain(context.Background(), handle, nil, nil,
		newChainBoundState(), nil, verifier, newClusterPolicyVerifier(), func(*servicepb.CheckStoreEvent) {})
	require.NoError(t, err)

	require.NotContains(t, verifier.keys, "legacy-key",
		"a legacy replay reference outside the entry's fresh-log window must not resurrect a revoked key")
	require.True(t, verifier.requireSignatures,
		"the entry's own fresh item must still fold, or the window guard is over-skipping")
}

// TestSigningVerifier_FoldArchivedIgnoresLegacyReplayReferences is the
// cold-storage twin of the test above.
//
// The archived fold needs the same fresh-log window as the live one, and for the
// same reason: an upgraded store's archived chapters can hold legacy
// pre-f9ee1e829 per-order replay references. Replaying one re-applies a register
// after the revoke that removed it, and because pre-boundary signing state has no
// other audit-derived source — the baseline checkpoint is a copy of the very
// projection under test — nothing downstream would catch the resurrected key.
func TestSigningVerifier_FoldArchivedIgnoresLegacyReplayReferences(t *testing.T) {
	t.Parallel()

	publicKey := signingTestKey(0x44)

	register := marshalSigningOrder(t, registerSigningKeyOrder("legacy-key", publicKey, ""))

	entry := func(seq, logSeq uint64) *auditpb.AuditEntry {
		return &auditpb.AuditEntry{
			Sequence:   seq,
			OrderCount: 1,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: logSeq, MaxLogSequence: logSeq},
			},
		}
	}

	sst := buildColdAuditSST(t,
		[]*auditpb.AuditEntry{entry(1, 10), entry(2, 11), entry(3, 12)},
		map[uint64][]*auditpb.AuditItem{
			1: {{OrderIndex: 0, LogSequence: 10, SerializedOrder: register}},
			2: {{
				OrderIndex:      0,
				LogSequence:     11,
				SerializedOrder: marshalSigningOrder(t, revokeSigningKeyOrder("legacy-key", false)),
			}},
			3: {
				{
					OrderIndex:      0,
					LogSequence:     12,
					SerializedOrder: marshalSigningOrder(t, setSigningConfigOrder(true)),
				},
				// The legacy shape: a replay reference back at log 10, carried by an
				// entry whose own fresh log is 12.
				{OrderIndex: 1, LogSequence: 10, SerializedOrder: register},
			},
		})

	verifier := newSigningVerifier()
	coldReader := coldReaderWithChapters(t, "signing-legacy-replay-bucket", map[uint64][]byte{4: sst})

	require.NoError(t, verifier.foldArchived(context.Background(),
		[]*commonpb.Chapter{signingChapter(4, commonpb.ChapterStatus_CHAPTER_ARCHIVED, 20, 3)},
		coldReader, logging.Testing()))

	require.True(t, verifier.coldComplete)
	require.Empty(t, verifier.keys,
		"a legacy replay reference outside its entry's fresh-log window must not resurrect the revoked key")
	require.True(t, verifier.requireSignatures,
		"the entry's own fresh item must still fold, or the window guard is over-skipping")
}
