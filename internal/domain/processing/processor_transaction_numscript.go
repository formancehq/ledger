package processing

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	numscriptlib "github.com/formancehq/numscript"
	"github.com/holiman/uint256"
)

type numscriptPostingProducer struct {
	cache  *numscript.NumscriptCache
	ledger string
}

func (p *numscriptPostingProducer) produce(s InMemoryStore, ledger string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	if order.Script == nil || order.Script.Plain == "" {
		return nil, numscript.ErrScriptRequired
	}

	// Parse the script (uses cache to avoid re-parsing)
	parsed, err := p.cache.GetOrParse(order.Script.Plain)
	if err != nil {
		return nil, err
	}

	// Build variables map from script vars
	vars := make(numscriptlib.VariablesMap)
	for k, v := range order.Script.Vars {
		vars[k] = v
	}

	// Create the store adapter
	// When Force is true, the adapter returns unlimited balances to bypass balance checks
	storeAdapter := &numscriptStoreAdapter{
		store:  s,
		ledger: ledger,
		force:  order.Force,
	}

	// Execute the script (experimental features are declared directly in scripts)
	result, err := parsed.Run(context.Background(), vars, storeAdapter)
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
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
		if sourceVol == nil {
			sourceVol = &raftcmdpb.VolumePair{}
		}
		addToVolumeSide(&sourceVol.OutputKnown, &sourceVol.OutputDiff, &u256Amount, postings[i].Amount, &scratch)
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
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
		if destVol == nil {
			destVol = &raftcmdpb.VolumePair{}
		}
		addToVolumeSide(&destVol.InputKnown, &destVol.InputDiff, &u256Amount, postings[i].Amount, &scratch)
		s.PutVolume(destKey, destVol)
	}

	// Apply account metadata from script execution and collect for return.
	// Build typed []*commonpb.Metadata directly to avoid map[string]string roundtrip.
	var accountsMeta map[string][]*commonpb.Metadata
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string][]*commonpb.Metadata, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			mdList := make([]*commonpb.Metadata, 0, len(meta))
			for key, value := range meta {
				mv := commonpb.NewStringValue(value)
				mdList = append(mdList, &commonpb.Metadata{Key: key, Value: mv})
				s.PutAccountMetadata(domain.MetadataKey{
					AccountKey: domain.AccountKey{
						Ledger:  ledger,
						Account: account,
					},
					Key: key,
				}, mv)
			}
			accountsMeta[account] = mdList
		}
	}

	// Convert transaction metadata from Numscript values to typed []*commonpb.Metadata.
	var txMeta []*commonpb.Metadata
	if len(result.Metadata) > 0 {
		txMeta = make([]*commonpb.Metadata, 0, len(result.Metadata))
		for key, value := range result.Metadata {
			txMeta = append(txMeta, &commonpb.Metadata{
				Key:   key,
				Value: commonpb.NewStringValue(value.String()),
			})
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
	store  InMemoryStore
	ledger string
	force  bool // When true, return unlimited balances to bypass balance checks
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
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, err
			}

			// Volumes must be preloaded by the admission layer.
			if vol == nil || (vol.InputKnown == nil && vol.InputDiff == nil) {
				return nil, &numscript.ErrBalanceNotPreloaded{Account: account, Asset: asset}
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
				// The schema is looked up lazily from the Store to avoid impacting
				// tests that don't set up the GetLedger expectation.
				if s.ledger != "" {
					if info, ok := s.store.GetLedger(s.ledger); ok && info.MetadataSchema != nil {
						if fields := info.MetadataSchema.AccountFields; fields != nil {
							if fieldSchema, schemaOK := fields[key]; schemaOK && !commonpb.TypeMatches(value, fieldSchema.Type) {
								value = commonpb.ConvertMetadataValue(value, fieldSchema.Type)
								s.store.PutAccountMetadata(metaKey, value)
							}
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
