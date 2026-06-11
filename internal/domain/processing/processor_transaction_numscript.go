package processing

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

type numscriptPostingProducer struct {
	cache      *numscript.NumscriptCache
	ledgerID   uint32
	schema     *commonpb.MetadataSchema
	assetCache map[string]cachedAssetPrecision
}

func (p *numscriptPostingProducer) produce(s InMemoryStore, ledgerID uint32, order *raftcmdpb.CreateTransactionOrder, script *commonpb.Script) (*produceResult, error) {
	if script == nil || script.GetPlain() == "" {
		return nil, domain.ErrScriptRequired
	}

	// Parse the script (uses cache to avoid re-parsing)
	parsed, err := p.cache.GetOrParse(script.GetPlain())
	if err != nil {
		return nil, err
	}

	// Build variables map from script vars
	vars := make(numscriptlib.VariablesMap)
	maps.Copy(vars, script.GetVars())

	// Create the store adapter
	// When Force is true, the adapter returns unlimited balances to bypass balance checks
	storeAdapter := &numscriptStoreAdapter{
		store:    s,
		ledgerID: ledgerID,
		force:    order.GetForce(),
		schema:   p.schema,
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
		sum        uint256.Int
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
		sourceKey := domain.NewVolumeKey(ledgerID, posting.Source, posting.Asset)

		sourceReader, err := s.GetVolume(sourceKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrBalanceNotPreloaded{Account: posting.Source, Asset: posting.Asset}
			}

			return nil, fmt.Errorf("source volume %s/%s: %w", posting.Source, posting.Asset, err)
		}
		if sourceReader == nil || sourceReader.GetInput() == nil || sourceReader.GetOutput() == nil {
			return nil, fmt.Errorf("source volume %s/%s not fully materialized", posting.Source, posting.Asset)
		}

		sourceVol := sourceReader.Mutate()
		sourceVol.GetOutput().IntoUint256(&scratch)

		// AddOverflow: plain Add would wrap silently and let extreme
		// Numscript-driven postings silently destroy funds. See #321.
		if _, overflow := sum.AddOverflow(&scratch, &u256Amount); overflow {
			return nil, &domain.ErrVolumeOverflow{
				Account: posting.Source,
				Asset:   posting.Asset,
				Side:    "output",
				Amount:  u256Amount.Dec(),
				Current: scratch.Dec(),
			}
		}

		sourceVol.GetOutput().SetFromUint256(&sum)
		s.PutVolume(sourceKey, sourceVol)

		// Update destination input (money coming in)
		destKey := domain.NewVolumeKey(ledgerID, posting.Destination, posting.Asset)

		destReader, err := s.GetVolume(destKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrBalanceNotPreloaded{Account: posting.Destination, Asset: posting.Asset}
			}

			return nil, fmt.Errorf("destination volume %s/%s: %w", posting.Destination, posting.Asset, err)
		}
		if destReader == nil || destReader.GetInput() == nil || destReader.GetOutput() == nil {
			return nil, fmt.Errorf("destination volume %s/%s not fully materialized", posting.Destination, posting.Asset)
		}

		destVol := destReader.Mutate()
		destVol.GetInput().IntoUint256(&scratch)

		if _, overflow := sum.AddOverflow(&scratch, &u256Amount); overflow {
			return nil, &domain.ErrVolumeOverflow{
				Account: posting.Destination,
				Asset:   posting.Asset,
				Side:    "input",
				Amount:  u256Amount.Dec(),
				Current: scratch.Dec(),
			}
		}

		destVol.GetInput().SetFromUint256(&sum)
		s.PutVolume(destKey, destVol)
	}

	// Collect account metadata from script execution for return. The caller
	// (processCreateTransaction) is responsible for capturing previous values
	// and writing the new ones — writing here would clobber the previous
	// value before the caller's GetAccountMetadata sees it, so the log's
	// PreviousAccountMetadata would equal the new metadata and the
	// indexbuilder could not remove stale index entries (#186).
	// Validate Numscript-produced metadata keys before they reach the
	// canonical Pebble key layout. set_account_meta / set_tx_meta keys
	// never pass through admission's ValidateMetadataKey, so an empty or
	// NUL-bearing key from a Numscript program would otherwise corrupt
	// read-index entries (#322).
	var accountsMeta map[string]map[string]*commonpb.MetadataValue
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]*commonpb.MetadataValue, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			mdMap := make(map[string]*commonpb.MetadataValue, len(meta))
			for key, value := range meta {
				if err := domain.ValidateMetadataKey(key); err != nil {
					return nil, fmt.Errorf("numscript-produced account %q metadata key: %w", account, err)
				}
				if err := domain.ValidateMetadataStringValue(value); err != nil {
					return nil, fmt.Errorf("numscript-produced account %q metadata key %q value: %w", account, key, err)
				}

				mdMap[key] = commonpb.NewStringValue(value)
			}

			accountsMeta[account] = mdMap
		}
	}

	// Convert transaction metadata from Numscript values to typed map.
	var txMeta map[string]*commonpb.MetadataValue
	if len(result.Metadata) > 0 {
		txMeta = make(map[string]*commonpb.MetadataValue, len(result.Metadata))
		for key, value := range result.Metadata {
			if err := domain.ValidateMetadataKey(key); err != nil {
				return nil, fmt.Errorf("numscript-produced transaction metadata key: %w", err)
			}

			stringValue := value.String()
			if err := domain.ValidateMetadataStringValue(stringValue); err != nil {
				return nil, fmt.Errorf("numscript-produced transaction metadata key %q value: %w", key, err)
			}

			txMeta[key] = commonpb.NewStringValue(stringValue)
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
	store    InMemoryStore
	ledgerID uint32
	force    bool // When true, return unlimited balances to bypass balance checks
	schema   *commonpb.MetadataSchema
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

			volumeKey := domain.NewVolumeKey(s.ledgerID, account, asset)

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
					LedgerID: s.ledgerID,
					Account:  account,
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
