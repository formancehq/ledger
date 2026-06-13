package processing

import (
	"errors"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

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
func cachedVolumeKey(ledgerID uint32, account, asset string, assetCache map[string]cachedAssetPrecision) domain.VolumeKey {
	if assetCache == nil {
		return domain.NewVolumeKey(ledgerID, account, asset)
	}

	cached, ok := assetCache[asset]
	if !ok {
		cached.base, cached.precision = domain.ParseAssetPrecision(asset)
		assetCache[asset] = cached
	}

	return domain.VolumeKey{
		AccountKey:     domain.AccountKey{LedgerID: ledgerID, Account: account},
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
func applyPosting(s InMemoryStore, ledgerID uint32, posting *commonpb.Posting, skipBalanceCheck bool, assetCache map[string]cachedAssetPrecision) domain.Describable {
	sourceKey := cachedVolumeKey(ledgerID, posting.GetSource(), posting.GetAsset(), assetCache)

	// Decode posting amount into stack variable to avoid heap allocation
	var amount uint256.Int
	posting.GetAmount().IntoUint256(&amount)

	// Get current volume pair for source — must be preloaded
	sourceReader, err := s.GetVolume(sourceKey)
	if err != nil {
		// ErrNotFound means the admission layer didn't preload this volume
		// — a precondition the caller can satisfy. Anything else is an
		// internal storage/cache failure the client cannot fix; surface it
		// as ErrStorageOperation instead of masking it as a preload miss
		// (paul-nicolas/NumaryBot review on #432).
		if errors.Is(err, domain.ErrNotFound) {
			return &domain.ErrBalanceNotPreloaded{Account: posting.GetSource(), Asset: posting.GetAsset()}
		}

		return &domain.ErrStorageOperation{Operation: "loading source volume", Cause: err}
	}
	if sourceReader == nil || sourceReader.GetInput() == nil || sourceReader.GetOutput() == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetSource(), Asset: posting.GetAsset()}
	}

	// Balance check (skip for "world" account and when skipBalanceCheck is true)
	if !skipBalanceCheck && posting.GetSource() != "world" {
		var inputValue uint256.Int
		sourceReader.GetInput().IntoUint256(&inputValue)

		var outputValue, outputPlusAmount uint256.Int
		sourceReader.GetOutput().IntoUint256(&outputValue)

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
	sourceVol := sourceReader.Mutate()
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
	s.PutVolume(sourceKey, sourceVol)

	// Destination receives credit - increase Input
	destKey := cachedVolumeKey(ledgerID, posting.GetDestination(), posting.GetAsset(), assetCache)

	destReader, err := s.GetVolume(destKey)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return &domain.ErrBalanceNotPreloaded{Account: posting.GetDestination(), Asset: posting.GetAsset()}
		}

		return &domain.ErrStorageOperation{Operation: "loading destination volume", Cause: err}
	}
	if destReader == nil || destReader.GetInput() == nil || destReader.GetOutput() == nil {
		return &domain.ErrBalanceNotPreloaded{Account: posting.GetDestination(), Asset: posting.GetAsset()}
	}

	destVol := destReader.Mutate()
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
	s.PutVolume(destKey, destVol)

	return nil
}
