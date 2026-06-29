package processing

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"strings"

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
	assetCache map[string]cachedAssetPrecision
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
			Color:       posting.Color,
		}

		// Update source output (money going out)
		sourceKey := domain.NewVolumeKey(ledgerName, posting.Source, posting.Asset, posting.Color)

		sourceReader, err := readVolumeOrZero(s, sourceKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{
				Operation: fmt.Sprintf("source volume %s/%s color=%q", posting.Source, posting.Asset, posting.Color),
				Cause:     err,
			}
		}
		if sourceReader == nil || sourceReader.GetInput() == nil || sourceReader.GetOutput() == nil {
			return nil, &domain.ErrVolumeNotMaterialized{
				Account: posting.Source,
				Asset:   posting.Asset,
				Color:   posting.Color,
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
				Color:   posting.Color,
				Side:    "output",
				Amount:  u256Amount.Dec(),
				Current: scratch.Dec(),
			}
		}

		sourceVol.GetOutput().SetFromUint256(&sum)
		s.Volumes().Put(sourceKey, sourceVol)

		// Update destination input (money coming in)
		destKey := domain.NewVolumeKey(ledgerName, posting.Destination, posting.Asset, posting.Color)

		destReader, err := readVolumeOrZero(s, destKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{
				Operation: fmt.Sprintf("destination volume %s/%s color=%q", posting.Destination, posting.Asset, posting.Color),
				Cause:     err,
			}
		}
		if destReader == nil || destReader.GetInput() == nil || destReader.GetOutput() == nil {
			return nil, &domain.ErrVolumeNotMaterialized{
				Account: posting.Destination,
				Asset:   posting.Asset,
				Color:   posting.Color,
				Side:    "destination",
			}
		}

		destVol := destReader.Mutate()
		destVol.GetInput().IntoUint256(&scratch)

		if _, overflow := sum.AddOverflow(&scratch, &u256Amount); overflow {
			return nil, &domain.ErrVolumeOverflow{
				Account: posting.Destination,
				Asset:   posting.Asset,
				Color:   posting.Color,
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
	var accountsMeta map[string]map[string]*commonpb.MetadataValue
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]*commonpb.MetadataValue, len(result.AccountsMetadata))
		for account, meta := range result.AccountsMetadata {
			mdMap := make(map[string]*commonpb.MetadataValue, len(meta))
			for key, value := range meta {
				if err := domain.ValidateMetadataKey(key); err != nil {
					return nil, &domain.ErrAccountValidation{Account: account, Cause: err}
				}
				if err := domain.ValidateMetadataStringValue(value); err != nil {
					return nil, &domain.ErrAccountValidation{Account: account, Cause: err}
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
				return nil, err
			}

			stringValue := value.String()
			if err := domain.ValidateMetadataStringValue(stringValue); err != nil {
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

// numscriptStoreAdapter adapts the Store interface to the numscript.Store interface.
type numscriptStoreAdapter struct {
	store      Scope
	ledgerName string
	force      bool // When true, return unlimited balances to bypass balance checks
}

func (s *numscriptStoreAdapter) GetBalances(_ context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	balances := make(numscriptlib.Balances, 0, len(query))

	var inputVal, outputVal uint256.Int // stack scratch reused across iterations

	for _, item := range query {
		// Reject the numscript runtime's catch-all asset query (`BASE/*`)
		// explicitly: the in-memory store does not expose iteration, so we
		// cannot expand the wildcard to the concrete precision flavors that
		// live on the account. Surface the unsupported case loudly rather
		// than letting readVolumeOrZero miss on a literal "BASE/*" key.
		if strings.HasSuffix(item.Asset, "/*") {
			return nil, numscript.ErrCatchAllAssetNotSupported
		}

		// When force mode is enabled, return unlimited balance for the
		// queried (account, asset, color) tuple. This bypasses balance
		// checks inside numscript while still respecting the color
		// dimension numscript will use to assemble postings.
		if s.force {
			balances = append(balances, numscriptlib.BalanceRow{
				Account: item.Account,
				Asset:   item.Asset,
				Color:   item.Color,
				Amount:  new(big.Int).Set(numscript.MaxForceBalance),
			})

			continue
		}

		volumeKey := domain.NewVolumeKey(s.ledgerName, item.Account, item.Asset, item.Color)

		vol, err := readVolumeOrZero(s.store, volumeKey)
		if err != nil {
			return nil, err
		}

		// Mirrors the guard in applyPosting (processor_posting.go) and produce()
		// above: WriteSet.GetVolume legitimately returns (nil, nil) for a key the
		// admission layer never preloaded (e.g. a colored bucket touched by a
		// catch-all expansion that didn't preload everything). Calling GetInput()
		// on a nil interface panics in the FSM apply path and desyncs the cluster.
		if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil {
			return nil, &domain.ErrBalanceNotPreloaded{Account: item.Account, Asset: item.Asset, Color: item.Color}
		}

		// Calculate balance: Input - Output using uint256, then convert to *big.Int at boundary
		vol.GetInput().IntoUint256(&inputVal)
		vol.GetOutput().IntoUint256(&outputVal)

		// balance escapes into the row, so it must be heap-allocated
		// Convert to *big.Int at the numscript boundary (numscript uses *big.Int)
		balances = append(balances, numscriptlib.BalanceRow{
			Account: item.Account,
			Asset:   item.Asset,
			Color:   item.Color,
			Amount:  new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig()),
		})
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
					LedgerName: s.ledgerName,
					Account:    account,
				},
				Key: key,
			}

			valueReader, err := s.store.AccountMetadata().Get(metaKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, err
			}

			if valueReader != nil {
				// Numscript sees the verbatim client write — declared_type is
				// an index hint only and MUST NOT influence script behaviour.
				// A previous version coerced "030" under a UINT64 declaration
				// to "30" here, which broke the lossless contract and let a
				// retype silently change transaction outcomes.
				str := commonpb.MetadataValueToString(valueReader.Mutate())
				if str != "" {
					accountMeta[key] = str
				}
			}
		}
	}

	return result, nil
}
