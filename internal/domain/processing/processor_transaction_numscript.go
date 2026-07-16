package processing

import (
	"context"
	"encoding/json"
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
	ledgerName string
}

func (p *numscriptPostingProducer) produce(s Scope, ledgerName string, order *raftcmdpb.CreateTransactionOrder, script *commonpb.Script) (*produceResult, domain.Describable) {
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
		store:      s,
		ledgerName: ledgerName,
		force:      order.GetForce(),
	}

	// Execute the script (experimental features are declared directly in scripts)
	result, err := numscript.SafeRun(parsed, context.Background(), vars, storeAdapter)
	if err != nil {
		// SafeRun already converted to Describable: ErrInsufficientFunds
		// for missing-funds, ErrNumscriptRuntime for panics and unmapped
		// library errors.
		return nil, err
	}

	return applyNumscriptResult(s, ledgerName, result)
}

// applyNumscriptResult converts a numscript execution result into ledger
// postings, updating the coverage-gated volume buffer, and collects the
// script-set transaction / account metadata. It is shared by the tree-walking
// interpreter producer and the bytecode VM producer: both return the same
// numscriptlib.ExecutionResult, so the posting→volume application is identical.
func applyNumscriptResult(s Scope, ledgerName string, result numscriptlib.ExecutionResult) (*produceResult, domain.Describable) {
	// Convert numscript postings to commonpb postings and update buffer
	postings := make([]*commonpb.Posting, len(result.Postings))

	var (
		scratch    uint256.Int // reused across all postings
		sum        uint256.Int
		u256Amount uint256.Int
	)

	for i, posting := range result.Postings {
		if posting.Amount.Sign() < 0 {
			return nil, &domain.ErrNumscriptRuntime{
				Detail: fmt.Sprintf("posting %d has negative amount %s", i, posting.Amount),
			}
		}

		if overflow := u256Amount.SetFromBig(posting.Amount); overflow {
			return nil, &domain.ErrNumscriptRuntime{
				Detail: fmt.Sprintf("posting %d amount %s exceeds 256 bits", i, posting.Amount),
			}
		}

		postings[i] = &commonpb.Posting{
			Source:      posting.Source,
			Destination: posting.Destination,
			Amount:      commonpb.NewUint256(&u256Amount),
			Asset:       posting.Asset,
		}

		// Update source output (money going out)
		sourceKey := domain.NewVolumeKey(ledgerName, posting.Source, posting.Asset)

		sourceReader, err := readVolumeOrZero(s, sourceKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{
				Operation: fmt.Sprintf("source volume %s/%s", posting.Source, posting.Asset),
				Cause:     err,
			}
		}
		if sourceReader == nil || sourceReader.GetInput() == nil || sourceReader.GetOutput() == nil {
			return nil, &domain.ErrVolumeNotMaterialized{
				Account: posting.Source,
				Asset:   posting.Asset,
				Side:    "source",
			}
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
		s.Volumes().Put(sourceKey, sourceVol)

		// Update destination input (money coming in)
		destKey := domain.NewVolumeKey(ledgerName, posting.Destination, posting.Asset)

		destReader, err := readVolumeOrZero(s, destKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{
				Operation: fmt.Sprintf("destination volume %s/%s", posting.Destination, posting.Asset),
				Cause:     err,
			}
		}
		if destReader == nil || destReader.GetInput() == nil || destReader.GetOutput() == nil {
			return nil, &domain.ErrVolumeNotMaterialized{
				Account: posting.Destination,
				Asset:   posting.Asset,
				Side:    "destination",
			}
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
		s.Volumes().Put(destKey, destVol)
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
	//
	// numscript now returns set account metadata as a flat list of rows
	// (SetAccountMetadataRow) carrying a typed Value; we group them back per
	// account and stringify the value for storage.
	var accountsMeta map[string]map[string]*commonpb.MetadataValue
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]*commonpb.MetadataValue, len(result.AccountsMetadata))
		for _, row := range result.AccountsMetadata {
			if err := domain.ValidateMetadataKey(row.Key); err != nil {
				return nil, &domain.ErrAccountValidation{Account: row.Account, Cause: err}
			}

			value := numscriptMetaValueToString(row.Value)
			if err := domain.ValidateMetadataString(value); err != nil {
				return nil, &domain.ErrAccountValidation{Account: row.Account, Cause: err}
			}

			mdMap := accountsMeta[row.Account]
			if mdMap == nil {
				mdMap = make(map[string]*commonpb.MetadataValue)
				accountsMeta[row.Account] = mdMap
			}

			mdMap[row.Key] = commonpb.NewStringValue(value)
		}
	}

	// Convert transaction metadata from Numscript values to typed map.
	var txMeta map[string]*commonpb.MetadataValue
	if len(result.Metadata) > 0 {
		txMeta = make(map[string]*commonpb.MetadataValue, len(result.Metadata))
		for key, value := range result.Metadata {
			if err := domain.ValidateMetadataKey(key); err != nil {
				return nil, err
			}

			stringValue := numscriptMetaValueToString(value)
			if err := domain.ValidateMetadataString(stringValue); err != nil {
				return nil, &domain.ErrMetadataKeyValidation{Key: key, Cause: err}
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

// numscriptMetaValueToString renders a script-set metadata Value as the raw
// string the ledger stores. numscript's Value.String() wraps plain strings in
// quotes (its canonical source form, e.g. `"savings"`), which would break the
// ledger's verbatim metadata contract — so string values are unwrapped to their
// raw content via numscript's own tagged-JSON serialization. All other value
// types (number, monetary, portion, asset, account) render unquoted through
// String() and are passed through unchanged.
func numscriptMetaValueToString(v numscriptlib.Value) string {
	// Value.MarshalJSON always succeeds for the closed set of numscript value
	// types; on the impossible marshal/unmarshal failure fall back to String().
	raw, err := json.Marshal(v)
	if err != nil {
		return v.String()
	}

	var tagged struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &tagged); err != nil {
		return v.String()
	}

	if tagged.Type == "string" {
		return tagged.Value
	}

	return v.String()
}

// numscriptBalance computes an account's numscript-visible balance
// (Input - Output) for one asset from the coverage-gated Scope. Force mode
// short-circuits to an effectively-infinite balance so balance checks are
// bypassed. A declared-but-absent volume (readVolumeOrZero → zero pair) is a
// fresh zero balance; a volume present but not materialized is a preload
// contract violation. Shared by the interpreter batch adapter and the VM
// single-lookup adapter.
func numscriptBalance(s Scope, ledgerName, account, asset string, force bool) (*big.Int, error) {
	if force {
		return new(big.Int).Set(numscript.MaxForceBalance), nil
	}

	vol, err := readVolumeOrZero(s, domain.NewVolumeKey(ledgerName, account, asset))
	if err != nil {
		return nil, err
	}

	if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil {
		return nil, &domain.ErrBalanceNotPreloaded{Account: account, Asset: asset}
	}

	var inputVal, outputVal uint256.Int
	vol.GetInput().IntoUint256(&inputVal)
	vol.GetOutput().IntoUint256(&outputVal)

	// balance escapes to numscript (which uses *big.Int), so heap-allocate.
	return new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig()), nil
}

// numscriptAccountMetadata reads a single account-metadata value for numscript
// meta() resolution. It returns the verbatim client write (declared_type is an
// index hint only and MUST NOT influence script behaviour — coercing e.g. "030"
// under a UINT64 declaration to "30" would break the lossless contract and let
// a retype silently change transaction outcomes). Not-found and empty values
// resolve to (,"", false). Shared by the interpreter batch adapter and the VM
// single-lookup adapter.
func numscriptAccountMetadata(s Scope, ledgerName, account, key string) (string, bool, error) {
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{
			LedgerName: ledgerName,
			Account:    account,
		},
		Key: key,
	}

	valueReader, err := s.AccountMetadata().Get(metaKey)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return "", false, err
	}

	if valueReader == nil {
		return "", false, nil
	}

	str := commonpb.MetadataValueToString(valueReader.Mutate())
	if str == "" {
		return "", false, nil
	}

	return str, true, nil
}

// numscriptStoreAdapter adapts the coverage-gated Scope to the numscript
// interpreter Store interface (batched balance / metadata queries).
type numscriptStoreAdapter struct {
	store      Scope
	ledgerName string
	force      bool // When true, return unlimited balances to bypass balance checks
}

func (s *numscriptStoreAdapter) GetBalances(_ context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	balances := make(numscriptlib.Balances, 0, len(query))

	for _, item := range query {
		balance, err := numscriptBalance(s.store, s.ledgerName, item.Account, item.Asset, s.force)
		if err != nil {
			return nil, err
		}

		// Echo back the query's color/scope so numscript can match the row to
		// the (account, asset, color, scope) slot it asked for.
		balances = append(balances, numscriptlib.BalanceRow{
			Account: item.Account,
			Asset:   item.Asset,
			Color:   item.Color,
			Scope:   item.Scope,
			Amount:  balance,
		})
	}

	return balances, nil
}

func (s *numscriptStoreAdapter) GetAccountsMetadata(_ context.Context, query numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	var result numscriptlib.AccountsMetadata

	for _, item := range query {
		for _, key := range item.Keys {
			value, ok, err := numscriptAccountMetadata(s.store, s.ledgerName, item.Account, key)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			result = append(result, numscriptlib.AccountMetadataRow{
				Account: item.Account,
				Key:     key,
				Value:   value,
				Scope:   item.Scope,
			})
		}
	}

	return result, nil
}
