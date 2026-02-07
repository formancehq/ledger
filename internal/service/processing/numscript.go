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

func (p *numscriptPostingProducer) produce(s Store, ledgerName string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	if order.Script == nil || order.Script.Plain == "" {
		return nil, errors.New("numscript: script is required")
	}

	// Parse the script (uses cache to avoid re-parsing)
	// todo: as parsing is an expensive process, we should either limit the number of the script of having an already parsed version of the AST in the raft entry
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
		store:      s,
		ledgerName: ledgerName,
		force:      order.Force,
	}

	// Execute the script with all feature flags enabled
	result, err := parsed.RunWithFeatureFlags(context.Background(), vars, storeAdapter, p.featureFlags)
	if err != nil {
		return nil, fmt.Errorf("numscript execution error: %w", err)
	}

	// Convert numscript postings to commonpb postings and update buffer
	postings := make([]*commonpb.Posting, len(result.Postings))
	for i, posting := range result.Postings {
		postings[i] = &commonpb.Posting{
			Source:      posting.Source,
			Destination: posting.Destination,
			Amount:      commonpb.NewBigInt(posting.Amount),
			Asset:       posting.Asset,
		}

		// Update source output (money going out)
		sourceKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerName: ledgerName,
				Account:    posting.Source,
			},
			Asset: posting.Asset,
		}
		sourceOutput, err := s.GetOutput(sourceKey)
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			return nil, err
		}
		if sourceOutput == nil {
			sourceOutput = &raftcmdpb.VolumeHolder{}
		}
		// If we know the absolute value, update Known (buffer.Merge will use SetBase).
		// If we don't know the absolute value, update DiffSinceBaseIndex (buffer.Merge will use AddDiff).
		if sourceOutput.Known != nil {
			sourceOutput.Known = commonpb.NewBigInt(
				new(big.Int).Add(sourceOutput.Known.Value(), posting.Amount),
			)
		} else {
			if sourceOutput.DiffSinceBaseIndex == nil {
				sourceOutput.DiffSinceBaseIndex = commonpb.NewBigInt(posting.Amount)
			} else {
				sourceOutput.DiffSinceBaseIndex = commonpb.NewBigInt(
					new(big.Int).Add(sourceOutput.DiffSinceBaseIndex.Value(), posting.Amount),
				)
			}
		}
		s.PutOutput(sourceKey, sourceOutput)

		// Update destination input (money coming in)
		destKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerName: ledgerName,
				Account:    posting.Destination,
			},
			Asset: posting.Asset,
		}
		destInput, err := s.GetInput(destKey)
		if err != nil && !errors.Is(err, data.ErrNotFound) {
			return nil, err
		}
		if destInput == nil {
			destInput = &raftcmdpb.VolumeHolder{}
		}
		// If we know the absolute value, update Known (buffer.Merge will use SetBase).
		// If we don't know the absolute value, update DiffSinceBaseIndex (buffer.Merge will use AddDiff).
		if destInput.Known != nil {
			destInput.Known = commonpb.NewBigInt(
				new(big.Int).Add(destInput.Known.Value(), posting.Amount),
			)
		} else {
			if destInput.DiffSinceBaseIndex == nil {
				destInput.DiffSinceBaseIndex = commonpb.NewBigInt(posting.Amount)
			} else {
				destInput.DiffSinceBaseIndex = commonpb.NewBigInt(
					new(big.Int).Add(destInput.DiffSinceBaseIndex.Value(), posting.Amount),
				)
			}
		}
		s.PutInput(destKey, destInput)
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
						LedgerName: ledgerName,
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
	store      Store
	ledgerName string
	force      bool // When true, return unlimited balances to bypass balance checks
}

// maxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account.
var maxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)

func (s *numscriptStoreAdapter) GetBalances(_ context.Context, query numscript.BalanceQuery) (numscript.Balances, error) {
	balances := make(numscript.Balances)

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
					LedgerName: s.ledgerName,
					Account:    account,
				},
				Asset: asset,
			}

			input, err := s.store.GetInput(volumeKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, err
			}

			output, err := s.store.GetOutput(volumeKey)
			if err != nil && !errors.Is(err, data.ErrNotFound) {
				return nil, err
			}

			// Volumes must be preloaded by the admission layer.
			// If not found, return an error - the client must ensure accounts are known.
			// Note: In the future, static analysis of Numscript will allow extracting
			// impacted accounts at admission time for automatic preloading.
			if input == nil || (input.Known == nil && input.DiffSinceBaseIndex == nil) {
				return nil, fmt.Errorf("balance not preloaded for account %q asset %q", account, asset)
			}

			// Calculate balance: Input - Output
			var inputValue, outputValue *big.Int
			if input.Known != nil {
				inputValue = input.Known.Value()
			} else {
				inputValue = input.DiffSinceBaseIndex.Value()
			}

			if output != nil && output.Known != nil {
				outputValue = output.Known.Value()
			} else if output != nil && output.DiffSinceBaseIndex != nil {
				outputValue = output.DiffSinceBaseIndex.Value()
			} else {
				outputValue = big.NewInt(0)
			}

			balance := new(big.Int).Sub(inputValue, outputValue)
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
					LedgerName: s.ledgerName,
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
