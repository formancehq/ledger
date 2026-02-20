package processing

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/numscript"
	"github.com/holiman/uint256"
)

type numscriptPostingProducer struct {
	cache        *NumscriptCache
	featureFlags map[string]struct{}
}

// numscriptFeatureFlags contains all experimental Numscript features that are enabled by default.
var numscriptFeatureFlags = map[string]struct{}{
	"experimental-account-interpolation":    {},
	"experimental-asset-colors":             {},
	"experimental-get-amount-function":      {},
	"experimental-get-asset-function":       {},
	"experimental-mid-script-function-call": {},
	"experimental-oneof":                    {},
	"experimental-overdraft-function":       {},
}

func (p *numscriptPostingProducer) produce(s Store, ledgerID uint32, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	if order.Script == nil || order.Script.Plain == "" {
		return nil, ErrScriptRequired
	}

	// Parse the script (uses cache to avoid re-parsing)
	parsed, err := p.cache.GetOrParse(order.Script.Plain)
	if err != nil {
		return nil, err
	}

	// Build variables map from script vars
	vars := make(numscript.VariablesMap)
	for k, v := range order.Script.Vars {
		vars[k] = v
	}

	// Create the store adapter
	// When Force is true, the adapter returns unlimited balances to bypass balance checks
	storeAdapter := &numscriptStoreAdapter{
		store:    s,
		ledgerID: ledgerID,
		force:    order.Force,
	}

	// Execute the script with all feature flags enabled
	result, err := parsed.RunWithFeatureFlags(context.Background(), vars, storeAdapter, p.featureFlags)
	if err != nil {
		return nil, fmt.Errorf("numscript execution error: %w", err)
	}

	// Convert numscript postings to commonpb postings and update buffer
	postings := make([]*commonpb.Posting, len(result.Postings))
	var (
		scratch   uint256.Int // reused across all postings
		u256Amount uint256.Int
	)
	for i, posting := range result.Postings {
		if posting.Amount.Sign() < 0 {
			return nil, fmt.Errorf("numscript execution error: posting %d has negative amount %s", i, posting.Amount)
		}
		if overflow := u256Amount.SetFromBig(posting.Amount); overflow {
			return nil, fmt.Errorf("numscript execution error: posting %d amount %s exceeds 256 bits", i, posting.Amount)
		}
		postings[i] = &commonpb.Posting{
			Source:      posting.Source,
			Destination: posting.Destination,
			Amount:      commonpb.NewUint256(&u256Amount),
			Asset:       posting.Asset,
		}

		// Update source output (money going out)
		sourceKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
				Account:  posting.Source,
			},
			Asset: posting.Asset,
		}
		sourceVol, err := s.GetVolume(sourceKey)
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			return nil, err
		}
		if sourceVol == nil {
			sourceVol = &raftcmdpb.VolumePair{}
		}
		addToVolumeSide(&sourceVol.OutputKnown, &sourceVol.OutputDiff, &u256Amount, postings[i].Amount, &scratch)
		s.PutVolume(sourceKey, sourceVol)

		// Update destination input (money coming in)
		destKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
				Account:  posting.Destination,
			},
			Asset: posting.Asset,
		}
		destVol, err := s.GetVolume(destKey)
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			return nil, err
		}
		if destVol == nil {
			destVol = &raftcmdpb.VolumePair{}
		}
		addToVolumeSide(&destVol.InputKnown, &destVol.InputDiff, &u256Amount, postings[i].Amount, &scratch)
		s.PutVolume(destKey, destVol)
	}

	// Apply account metadata from script execution and collect for return
	var accountsMeta map[string]map[string]string
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]string, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			accountsMeta[account] = make(map[string]string, len(meta))
			for key, value := range meta {
				accountsMeta[account][key] = value
				s.PutAccountMetadata(data.MetadataKey{
					AccountKey: data.AccountKey{
						LedgerID: ledgerID,
						Account:    account,
					},
					Key: key,
				}, &commonpb.MetadataValue{Value: value})
			}
		}
	}

	// Convert transaction metadata from Numscript values to strings
	var txMeta map[string]string
	if len(result.Metadata) > 0 {
		txMeta = make(map[string]string, len(result.Metadata))
		for key, value := range result.Metadata {
			txMeta[key] = value.String()
		}
	}

	return &produceResult{
		Postings:            postings,
		TransactionMetadata: txMeta,
		AccountsMetadata:    accountsMeta,
	}, nil
}

// numscriptStoreAdapter adapts the Store interface to the numscript.Store interface
type numscriptStoreAdapter struct {
	store    Store
	ledgerID uint32
	force    bool // When true, return unlimited balances to bypass balance checks
}

// maxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account.
var maxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)

func (s *numscriptStoreAdapter) GetBalances(_ context.Context, query numscript.BalanceQuery) (numscript.Balances, error) {
	balances := make(numscript.Balances)

	var inputVal, outputVal uint256.Int // stack scratch reused across iterations

	for account, assets := range query {
		accountBalance := make(numscript.AccountBalance)
		balances[account] = accountBalance

		for _, asset := range assets {
			// When force mode is enabled, return unlimited balance for all accounts
			// This bypasses all balance checks in Numscript execution
			if s.force {
				accountBalance[asset] = new(big.Int).Set(maxForceBalance)
				continue
			}

			volumeKey := data.VolumeKey{
				AccountKey: data.AccountKey{
					LedgerID: s.ledgerID,
					Account:  account,
				},
				Asset: asset,
			}

			vol, err := s.store.GetVolume(volumeKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, err
			}

			// Volumes must be preloaded by the admission layer.
			if vol == nil || (vol.InputKnown == nil && vol.InputDiff == nil) {
				return nil, &ErrBalanceNotPreloaded{Account: account, Asset: asset}
			}

			// Calculate balance: Input - Output using uint256, then convert to *big.Int at boundary
			if vol.InputKnown != nil {
				vol.InputKnown.IntoUint256(&inputVal)
			} else {
				vol.InputDiff.IntoUint256(&inputVal)
			}

			outputVal.Clear()
			if vol.OutputKnown != nil {
				vol.OutputKnown.IntoUint256(&outputVal)
			} else if vol.OutputDiff != nil {
				vol.OutputDiff.IntoUint256(&outputVal)
			}

			// balance escapes into the map, so it must be heap-allocated
			// Convert to *big.Int at the numscript boundary (numscript uses *big.Int)
			balance := new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig())
			accountBalance[asset] = balance
		}
	}

	return balances, nil
}

func (s *numscriptStoreAdapter) GetAccountsMetadata(_ context.Context, query numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	result := make(numscript.AccountsMetadata)

	for account, keys := range query {
		accountMeta := make(numscript.AccountMetadata)
		result[account] = accountMeta

		for _, key := range keys {
			metaKey := data.MetadataKey{
				AccountKey: data.AccountKey{
					LedgerID: s.ledgerID,
					Account:    account,
				},
				Key: key,
			}

			value, err := s.store.GetAccountMetadata(metaKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, err
			}
			if value != nil && value.Value != "" {
				accountMeta[key] = value.Value
			}
		}
	}

	return result, nil
}
