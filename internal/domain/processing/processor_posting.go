package processing

import (
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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
func applyPosting(s InMemoryStore, ledgerID uint32, posting *commonpb.Posting, skipBalanceCheck bool, assetCache map[string]cachedAssetPrecision) error {
	sourceKey := cachedVolumeKey(ledgerID, posting.GetSource(), posting.GetAsset(), assetCache)

	// Decode posting amount into stack variable to avoid heap allocation
	var amount uint256.Int
	posting.GetAmount().IntoUint256(&amount)

	// Get current volume pair for source — must be preloaded
	sourceVol, err := s.GetVolume(sourceKey)
	if err != nil {
		return fmt.Errorf("source volume %s/%s not preloaded: %w", posting.GetSource(), posting.GetAsset(), err)
	}
	if sourceVol == nil || sourceVol.GetInput() == nil || sourceVol.GetOutput() == nil {
		return fmt.Errorf("source volume %s/%s not fully preloaded", posting.GetSource(), posting.GetAsset())
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

	// scratch is reused across both volume updates
	var scratch uint256.Int

	// Increase Output for source (money going out)
	sourceVol.GetOutput().IntoUint256(&scratch)
	scratch.Add(&scratch, &amount)
	sourceVol.GetOutput().SetFromUint256(&scratch)
	s.PutVolume(sourceKey, sourceVol)

	// Destination receives credit - increase Input
	destKey := cachedVolumeKey(ledgerID, posting.GetDestination(), posting.GetAsset(), assetCache)

	destVol, err := s.GetVolume(destKey)
	if err != nil {
		return fmt.Errorf("destination volume %s/%s not preloaded: %w", posting.GetDestination(), posting.GetAsset(), err)
	}
	if destVol == nil || destVol.GetInput() == nil || destVol.GetOutput() == nil {
		return fmt.Errorf("destination volume %s/%s not fully preloaded", posting.GetDestination(), posting.GetAsset())
	}

	destVol.GetInput().IntoUint256(&scratch)
	scratch.Add(&scratch, &amount)
	destVol.GetInput().SetFromUint256(&scratch)
	s.PutVolume(destKey, destVol)

	return nil
}
