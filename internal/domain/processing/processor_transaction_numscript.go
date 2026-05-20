package processing

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

type numscriptPostingProducer struct {
	cache  *numscript.NumscriptCache
	ledger string
	schema *commonpb.MetadataSchema
}

func (p *numscriptPostingProducer) produce(s InMemoryStore, ledger string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	if order.GetScript() == nil || order.GetScript().GetPlain() == "" {
		return nil, domain.ErrScriptRequired
	}

	// Parse the script (uses cache to avoid re-parsing)
	parsed, err := p.cache.GetOrParse(order.GetScript().GetPlain())
	if err != nil {
		return nil, err
	}

	// Build variables map from script vars
	vars := make(numscriptlib.VariablesMap)
	maps.Copy(vars, order.GetScript().GetVars())

	// Create the store adapter
	// When Force is true, the adapter returns unlimited balances to bypass balance checks
	storeAdapter := &numscriptStoreAdapter{
		store:  s,
		ledger: ledger,
		force:  order.GetForce(),
		schema: p.schema,
	}

	// Execute the script (experimental features are declared directly in scripts)
	result, err := numscript.SafeRun(parsed, context.Background(), vars, storeAdapter)
	if err != nil {
		return nil, fmt.Errorf("numscript execution error: %w", err)
	}

	// Convert numscript postings to commonpb postings and update buffer
	postings := make([]*commonpb.Posting, len(result.Postings))

	var (
		scratch    uint256.Int // reused across all postings
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
		sourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.Source,
			},
			Asset: posting.Asset,
		}

		sourceVol, err := s.GetVolume(sourceKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrBalanceNotPreloaded{Account: posting.Source, Asset: posting.Asset}
			}

			return nil, fmt.Errorf("source volume %s/%s: %w", posting.Source, posting.Asset, err)
		}
		if sourceVol.GetInput() == nil || sourceVol.GetOutput() == nil {
			return nil, fmt.Errorf("source volume %s/%s not fully materialized", posting.Source, posting.Asset)
		}

		sourceVol.GetOutput().IntoUint256(&scratch)
		scratch.Add(&scratch, &u256Amount)
		sourceVol.GetOutput().SetFromUint256(&scratch)
		s.PutVolume(sourceKey, sourceVol)

		// Update destination input (money coming in)
		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.Destination,
			},
			Asset: posting.Asset,
		}

		destVol, err := s.GetVolume(destKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrBalanceNotPreloaded{Account: posting.Destination, Asset: posting.Asset}
			}

			return nil, fmt.Errorf("destination volume %s/%s: %w", posting.Destination, posting.Asset, err)
		}
		if destVol.GetInput() == nil || destVol.GetOutput() == nil {
			return nil, fmt.Errorf("destination volume %s/%s not fully materialized", posting.Destination, posting.Asset)
		}

		destVol.GetInput().IntoUint256(&scratch)
		scratch.Add(&scratch, &u256Amount)
		destVol.GetInput().SetFromUint256(&scratch)
		s.PutVolume(destKey, destVol)
	}

	// Apply account metadata from script execution and collect for return.
	var accountsMeta map[string]map[string]*commonpb.MetadataValue
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]*commonpb.MetadataValue, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			mdMap := make(map[string]*commonpb.MetadataValue, len(meta))
			for key, value := range meta {
				mv := commonpb.NewStringValue(value)
				mdMap[key] = mv
				s.PutAccountMetadata(domain.MetadataKey{
					AccountKey: domain.AccountKey{
						Ledger:  ledger,
						Account: account,
					},
					Key: key,
				}, mv)
			}

			accountsMeta[account] = mdMap
		}
	}

	// Convert transaction metadata from Numscript values to typed map.
	var txMeta map[string]*commonpb.MetadataValue
	if len(result.Metadata) > 0 {
		txMeta = make(map[string]*commonpb.MetadataValue, len(result.Metadata))
		for key, value := range result.Metadata {
			txMeta[key] = commonpb.NewStringValue(value.String())
		}
	}

	return &produceResult{
		Postings:            postings,
		TransactionMetadata: txMeta,
		AccountsMetadata:    accountsMeta,
	}, nil
}

// numscriptStoreAdapter adapts the Store interface to the numscript.Store interface.
type numscriptStoreAdapter struct {
	store  InMemoryStore
	ledger string
	force  bool // When true, return unlimited balances to bypass balance checks
	schema *commonpb.MetadataSchema
}

func (s *numscriptStoreAdapter) GetBalances(_ context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	balances := make(numscriptlib.Balances)

	var inputVal, outputVal uint256.Int // stack scratch reused across iterations

	for account, assets := range query {
		accountBalance := make(numscriptlib.AccountBalance)
		balances[account] = accountBalance

		for _, asset := range assets {
			// When force mode is enabled, return unlimited balance for all accounts
			// This bypasses all balance checks in Numscript execution
			if s.force {
				accountBalance[asset] = new(big.Int).Set(numscript.MaxForceBalance)

				continue
			}

			volumeKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{
					Ledger:  s.ledger,
					Account: account,
				},
				Asset: asset,
			}

			vol, err := s.store.GetVolume(volumeKey)
			if err != nil {
				if errors.Is(err, domain.ErrNotFound) {
					return nil, &domain.ErrBalanceNotPreloaded{Account: account, Asset: asset}
				}

				return nil, err
			}

			if vol.GetInput() == nil || vol.GetOutput() == nil {
				return nil, &domain.ErrBalanceNotPreloaded{Account: account, Asset: asset}
			}

			// Calculate balance: Input - Output using uint256, then convert to *big.Int at boundary
			vol.GetInput().IntoUint256(&inputVal)
			vol.GetOutput().IntoUint256(&outputVal)

			// balance escapes into the map, so it must be heap-allocated
			// Convert to *big.Int at the numscript boundary (numscript uses *big.Int)
			balance := new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig())
			accountBalance[asset] = balance
		}
	}

	return balances, nil
}

func (s *numscriptStoreAdapter) GetAccountsMetadata(_ context.Context, query numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	result := make(numscriptlib.AccountsMetadata)

	for account, keys := range query {
		accountMeta := make(numscriptlib.AccountMetadata)
		result[account] = accountMeta

		for _, key := range keys {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  s.ledger,
					Account: account,
				},
				Key: key,
			}

			value, err := s.store.GetAccountMetadata(metaKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, err
			}

			if value != nil {
				// Opportunistically convert to declared schema type and write back.
				if s.schema != nil {
					if fields := s.schema.GetAccountFields(); fields != nil {
						if fieldSchema, schemaOK := fields[key]; schemaOK && !commonpb.TypeMatches(value, fieldSchema.GetType()) {
							value = commonpb.ConvertMetadataValue(value, fieldSchema.GetType())
							s.store.PutAccountMetadata(metaKey, value)
						}
					}
				}

				str := commonpb.MetadataValueToString(value)
				if str != "" {
					accountMeta[key] = str
				}
			}
		}
	}

	return result, nil
}
