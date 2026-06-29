package state

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestVerifyVolumeUpdateMonotonicity_UndefinedOldIsAllowed pins the EN-1378
// contract for the volume monotonicity sentinel: a freshly-created
// (account, asset) under the Declare-on-absent admission contract has no
// prior cache entry, so KeyStore.Put returns Update.Old = kv.None. The
// sentinel must accept that as a zero baseline rather than panic the FSM
// pool with "preload missing" (the failure mode the e2e cluster suite
// hit during the EN-1378 roll-out).
func TestVerifyVolumeUpdateMonotonicity_UndefinedOldIsAllowed(t *testing.T) {
	t.Parallel()

	key := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: "users:bob"},
		Asset:      "USD",
	}

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          key,
			CanonicalKey: key.Bytes(),
			Old:          kv.None[*raftcmdpb.VolumePair](), // first-write: no prior entry
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(500),
				Output: commonpb.NewUint256FromUint64(0),
			},
		},
	}

	require.NoError(t, verifyVolumeUpdateMonotonicity(updates),
		"undefined Old must be treated as zero baseline, not as a preload-missing bug")
}

// TestVerifyVolumeDeltasMatchPostings_UndefinedOldZeroBaseline pins the
// same contract for the delta sentinel: a CreatedTransaction posting that
// touches a fresh (account, asset) pair produces an Update with Old=None
// and a New equal to the posting delta. The expected delta computed from
// the log must match (zero-baseline → delta == New values).
func TestVerifyVolumeDeltasMatchPostings_UndefinedOldZeroBaseline(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		amount = uint64(500)
	)

	// Use NewVolumeKey so AssetBase / AssetPrecision are pre-parsed and the
	// keys compare equal to the ones the sentinel rebuilds from postings.
	srcKey := domain.NewVolumeKey(ledger, "world", "USD")
	dstKey := domain.NewVolumeKey(ledger, "users:bob", "USD")

	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          srcKey,
			CanonicalKey: srcKey.Bytes(),
			Old:          kv.None[*raftcmdpb.VolumePair](),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(0),
				Output: commonpb.NewUint256FromUint64(amount),
			},
		},
		{
			Key:          dstKey,
			CanonicalKey: dstKey.Bytes(),
			Old:          kv.None[*raftcmdpb.VolumePair](),
			New: &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(amount),
				Output: commonpb.NewUint256FromUint64(0),
			},
		},
	}

	logs := []*commonpb.Log{
		{
			Sequence: 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{
								Transaction: commonpb.NewTransaction().
									WithPostings(commonpb.NewPosting("world", "users:bob", "USD", new(big.Int).SetUint64(amount))).
									WithID(1),
							},
						},
					}).WithID(1),
				},
			}},
		},
	}

	require.NoError(t, verifyVolumeDeltasMatchPostings(updates, logs),
		"undefined Old must yield a zero baseline so the computed delta matches the posting amount")
}
