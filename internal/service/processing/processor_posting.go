package processing

import (
	"errors"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// applyPosting applies a single posting by updating volumes.
// It checks the source balance (unless skipBalanceCheck is true or source is "world"),
// increases Output for source and Input for destination.
func applyPosting(s InMemoryStore, ledgerID uint32, posting *commonpb.Posting, skipBalanceCheck bool) error {
	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{
			LedgerID: ledgerID,
			Account:  posting.Source,
		},
		Asset: posting.Asset,
	}

	// Decode posting amount into stack variable to avoid heap allocation
	var amount uint256.Int
	posting.Amount.IntoUint256(&amount)

	// Get current volume pair for source
	sourceVol, err := s.GetVolume(sourceKey)
	if err != nil && !errors.Is(err, dal.ErrNotFound) {
		return err
	}
	if sourceVol == nil {
		sourceVol = &raftcmdpb.VolumePair{}
	}

	// Balance check (skip for "world" account and when skipBalanceCheck is true)
	if !skipBalanceCheck && posting.Source != "world" {
		// Compute effective input value. When a preload set InputKnown, it already
		// includes any prior InputDiff (see putInCacheVolumePair merge). When only
		// InputDiff is present (force TX volumes still in cache without preload),
		// the diff IS the total accumulated input.
		var inputValue uint256.Int
		if sourceVol.InputKnown != nil {
			sourceVol.InputKnown.IntoUint256(&inputValue)
		} else if sourceVol.InputDiff != nil {
			sourceVol.InputDiff.IntoUint256(&inputValue)
		} else {
			return &ErrBalanceNotFound{Account: posting.Source, Asset: posting.Asset}
		}

		// Compute effective output value with the same logic.
		var outputValue, outputPlusAmount uint256.Int
		if sourceVol.OutputKnown != nil {
			sourceVol.OutputKnown.IntoUint256(&outputValue)
		} else if sourceVol.OutputDiff != nil {
			sourceVol.OutputDiff.IntoUint256(&outputValue)
		}
		sum, overflow := outputPlusAmount.AddOverflow(&outputValue, &amount)
		if overflow || inputValue.Lt(sum) {
			// Only compute signed balance for the error message
			balanceBig := new(big.Int).Sub(inputValue.ToBig(), outputValue.ToBig())
			return &ErrInsufficientFunds{
				Account: posting.Source,
				Asset:   posting.Asset,
				Amount:  amount.Dec(),
				Balance: balanceBig.String(),
			}
		}
	}

	// scratch is reused across both addToVolumeSide calls
	var scratch uint256.Int

	// Increase Output for source (money going out)
	addToVolumeSide(&sourceVol.OutputKnown, &sourceVol.OutputDiff, &amount, posting.Amount, &scratch)
	s.PutVolume(sourceKey, sourceVol)

	// Destination receives credit - increase Input
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{
			LedgerID: ledgerID,
			Account:  posting.Destination,
		},
		Asset: posting.Asset,
	}

	destVol, err := s.GetVolume(destKey)
	if err != nil && !errors.Is(err, dal.ErrNotFound) {
		return err
	}
	if destVol == nil {
		destVol = &raftcmdpb.VolumePair{}
	}
	addToVolumeSide(&destVol.InputKnown, &destVol.InputDiff, &amount, posting.Amount, &scratch)
	s.PutVolume(destKey, destVol)

	return nil
}

// addToVolumeSide adds amount to one side (input or output) of a VolumePair.
// If Known is set, it updates Known (SetBase path). Otherwise, it updates Diff (AddDiff path).
// rawAmount is the original proto Uint256 used when Diff is nil to avoid re-encoding.
// scratch is a caller-provided uint256.Int to avoid heap allocation.
func addToVolumeSide(known **commonpb.Uint256, diff **commonpb.Uint256, amount *uint256.Int, rawAmount *commonpb.Uint256, scratch *uint256.Int) {
	if *known != nil {
		// Safe to mutate in-place: *known is always a cloned cache value, never shared.
		(*known).IntoUint256(scratch)
		scratch.Add(scratch, amount)
		(*known).SetFromUint256(scratch)
	} else {
		if *diff == nil {
			*diff = rawAmount
		} else {
			// Must create new *Uint256: *diff may point to a shared rawAmount (posting.Amount).
			(*diff).IntoUint256(scratch)
			scratch.Add(scratch, amount)
			*diff = commonpb.NewUint256(scratch)
		}
	}
}
