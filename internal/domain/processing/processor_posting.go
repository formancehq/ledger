package processing

import (
	"errors"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// zeroVolumePair is the canonical "fresh (account, asset)" balance. It is
// frozen at package load and only ever surfaced as a VolumePairReader, whose
// Mutate() returns a deep CloneVT — callers writing into the balance get an
// independent clone, the shared instance stays immutable.
var zeroVolumePair = &raftcmdpb.VolumePair{
	Input:  commonpb.NewUint256FromUint64(0),
	Output: commonpb.NewUint256FromUint64(0),
}

// readVolumeOrZero is the canonical helper for the FSM apply path's volume
// reads (EN-1378). Admission emits a Declare plan for volume keys it
// resolved as absent in Pebble — the FSM-side cache stays empty and
// Scope.Volumes().Get returns domain.ErrNotFound. By convention that is a
// fresh (account, asset) with zero balance, synthesised here so callers
// never special-case the absent path.
//
// A *state.ErrCoverageMiss (admission contract violation — the need was
// never declared) is NOT domain.ErrNotFound and propagates unchanged so
// the coverage gate keeps catching admission bugs.
func readVolumeOrZero(s Scope, key domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
	reader, err := s.Volumes().Get(key)
	if err == nil {
		return reader, nil
	}

	if errors.Is(err, domain.ErrNotFound) {
		return zeroVolumePair.AsReader(), nil
	}

	return nil, err
}

// cachedAssetPrecision holds pre-parsed asset base and precision to avoid
// redundant ParseAssetPrecision calls when the same asset appears across
// multiple postings in a batch.
type cachedAssetPrecision struct {
	base      string
	precision uint8
}

// cachedVolumeKey builds a VolumeKey, using the assetCache to avoid
// re-parsing the asset precision when the same asset string recurs.
// If assetCache is nil, falls back to domain.NewVolumeKey.
func cachedVolumeKey(ledgerName string, account, asset string, assetCache map[string]cachedAssetPrecision) domain.VolumeKey {
	if assetCache == nil {
		return domain.NewVolumeKey(ledgerName, account, asset)
	}

	cached, ok := assetCache[asset]
	if !ok {
		cached.base, cached.precision = domain.ParseAssetPrecision(asset)
		assetCache[asset] = cached
	}

	return domain.VolumeKey{
		AccountKey:     domain.AccountKey{LedgerName: ledgerName, Account: account},
		Asset:          asset,
		AssetBase:      cached.base,
		AssetPrecision: cached.precision,
	}
}

// applyPosting applies a single posting by updating volumes.
// It checks the source balance (unless skipBalanceCheck is true or source is "world"),
// increases Output for source and Input for destination.
// All volumes must be preloaded by the admission layer — nil volumes return an error.
// assetCache, if non-nil, avoids redundant ParseAssetPrecision calls across postings.
func applyPosting(s Scope, ledgerName string, posting *commonpb.Posting, skipBalanceCheck bool, assetCache map[string]cachedAssetPrecision) domain.Describable {
	sourceKey := cachedVolumeKey(ledgerName, posting.GetSource(), posting.GetAsset(), assetCache)

	// Decode posting amount into stack variable to avoid heap allocation
	var amount uint256.Int
	posting.GetAmount().IntoUint256(&amount)

	// Get current volume pair for source as a mutable *VolumePair. Clone
	// once up-front so the balance check reads from the mutable pointer
	// directly — each `sourceReader.GetInput()` / `.GetOutput()` on the
	// reader wrapper would allocate a fresh Uint256Reader per call
	// (~10 wrapper allocs per posting between the null checks and the
	// balance arithmetic). With the *VolumePair in hand, GetInput /
	// GetOutput return the underlying *Uint256 directly, zero allocation.
	//
	// readVolumeOrZero treats a declared-but-absent key as a fresh zero
	// balance (EN-1378); a coverage miss (admission contract violation)
	// propagates unchanged through the ErrStorageOperation wrap,
	// preserving the cause for downstream errors.As.
	sourceReader, err := readVolumeOrZero(s, sourceKey)
	if err != nil {
		return &domain.ErrStorageOperation{Operation: "loading source volume", Cause: err}
	}
	if sourceReader == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetSource(), Asset: posting.GetAsset()}
	}

	sourceVol := sourceReader.Mutate()
	if sourceVol.GetInput() == nil || sourceVol.GetOutput() == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetSource(), Asset: posting.GetAsset()}
	}

	// Balance check (skip for "world" account and when skipBalanceCheck is true)
	if !skipBalanceCheck && posting.GetSource() != "world" {
		var inputValue uint256.Int
		sourceVol.GetInput().IntoUint256(&inputValue)

		var outputValue, outputPlusAmount uint256.Int
		sourceVol.GetOutput().IntoUint256(&outputValue)

		sum, overflow := outputPlusAmount.AddOverflow(&outputValue, &amount)
		if overflow || inputValue.Lt(sum) {
			// Only compute signed balance for the error message
			balanceBig := new(big.Int).Sub(inputValue.ToBig(), outputValue.ToBig())

			return &domain.ErrInsufficientFunds{
				Account: posting.GetSource(),
				Asset:   posting.GetAsset(),
				Amount:  amount.Dec(),
				Balance: balanceBig.String(),
			}
		}
	}

	// scratch is reused across both volume updates. Each mutation uses
	// AddOverflow and rejects the order on overflow: plain uint256.Add
	// wraps silently and would let an extreme posting (e.g. world → A of
	// 2^256-1 followed by 1) wrap A.Input back to 0 while Output stayed
	// unchanged — money silently created or destroyed (#321). The FSM
	// apply path discards the WriteSet atomically on error, so erroring
	// is safe.
	var scratch, sum uint256.Int

	// Increase Output for source (money going out).
	sourceVol.GetOutput().IntoUint256(&scratch)

	if _, overflow := sum.AddOverflow(&scratch, &amount); overflow {
		return &domain.ErrVolumeOverflow{
			Account: posting.GetSource(),
			Asset:   posting.GetAsset(),
			Side:    "output",
			Amount:  amount.Dec(),
			Current: scratch.Dec(),
		}
	}

	sourceVol.GetOutput().SetFromUint256(&sum)
	s.Volumes().Put(sourceKey, sourceVol)

	// Destination receives credit - increase Input
	destKey := cachedVolumeKey(ledgerName, posting.GetDestination(), posting.GetAsset(), assetCache)

	destReader, err := readVolumeOrZero(s, destKey)
	if err != nil {
		return &domain.ErrStorageOperation{Operation: "loading destination volume", Cause: err}
	}
	if destReader == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetDestination(), Asset: posting.GetAsset()}
	}

	destVol := destReader.Mutate()
	if destVol.GetInput() == nil || destVol.GetOutput() == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetDestination(), Asset: posting.GetAsset()}
	}

	destVol.GetInput().IntoUint256(&scratch)

	if _, overflow := sum.AddOverflow(&scratch, &amount); overflow {
		return &domain.ErrVolumeOverflow{
			Account: posting.GetDestination(),
			Asset:   posting.GetAsset(),
			Side:    "input",
			Amount:  amount.Dec(),
			Current: scratch.Dec(),
		}
	}

	destVol.GetInput().SetFromUint256(&sum)
	s.Volumes().Put(destKey, destVol)

	return nil
}
