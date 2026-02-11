package processing

import (
	"context"
	"maps"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/numscript"
)

// discoveryStore implements numscript.Store to discover which accounts/assets
// a Numscript script queries. It returns infinite balances so the script
// executes fully, and records every account/asset pair queried via GetBalances.
//
// Determinism constraint: GetBalances and GetAccountsMetadata may each be called
// at most once. A second call indicates a non-deterministic script (e.g., mid-script
// balance queries) which cannot be reliably preloaded. The violation is recorded
// in the nonDeterministic field and checked after execution.
//
// This is a temporary workaround until the Numscript library implements static
// analysis of required inputs (see docs/drafts/numscript-static-inputs-rfc.md).
type discoveryStore struct {
	queried          map[data.VolumeKey]struct{}
	balancesCalled   bool
	metadataCalled   bool
	nonDeterministic *ErrNonDeterministicScript
}

func (s *discoveryStore) GetBalances(_ context.Context, query numscript.BalanceQuery) (numscript.Balances, error) {
	if s.balancesCalled {
		s.nonDeterministic = &ErrNonDeterministicScript{Method: "GetBalances"}
		return nil, s.nonDeterministic
	}
	s.balancesCalled = true

	balances := make(numscript.Balances, len(query))
	for account, assets := range query {
		accountBalance := make(numscript.AccountBalance, len(assets))
		balances[account] = accountBalance
		for _, asset := range assets {
			s.queried[data.VolumeKey{
				AccountKey: data.AccountKey{Account: account},
				Asset:      asset,
			}] = struct{}{}
			accountBalance[asset] = new(big.Int).Set(maxForceBalance)
		}
	}
	return balances, nil
}

func (s *discoveryStore) GetAccountsMetadata(_ context.Context, query numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	if s.metadataCalled {
		s.nonDeterministic = &ErrNonDeterministicScript{Method: "GetAccountsMetadata"}
		return nil, s.nonDeterministic
	}
	s.metadataCalled = true

	result := make(numscript.AccountsMetadata, len(query))
	for account := range query {
		result[account] = make(numscript.AccountMetadata)
	}
	return result, nil
}

// DiscoverNumscriptVolumes runs a Numscript script with a discovery store that
// returns infinite balances, solely to discover which accounts/assets the script
// queries. The returned volume keys have their LedgerID set to the provided value.
//
// Scripts must be deterministic: GetBalances and GetAccountsMetadata may each be
// called at most once. If a script calls either more than once (e.g., via mid-script
// balance queries), ErrNonDeterministicScript is returned.
//
// Other execution errors are ignored: the discovery store returns infinite balances
// which may cause the script to follow different code paths. We collect whatever
// accounts were queried before the error occurred.
//
// Known limitation: with infinite balances, `oneof` may only query the first source.
func DiscoverNumscriptVolumes(script string, vars map[string]string, ledgerID uint32) (map[data.VolumeKey]struct{}, error) {
	parsed := numscript.Parse(script)
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		return nil, &ErrNumscriptParse{
			Details: numscript.ParseErrorsToString(errs, parsed.GetSource()),
		}
	}

	variablesMap := make(numscript.VariablesMap, len(vars))
	maps.Copy(variablesMap, vars)

	store := &discoveryStore{
		queried: make(map[data.VolumeKey]struct{}),
	}

	// Run the script. The discovery store captures source accounts via GetBalances
	// and we extract destinations from the resulting postings.
	execResult, _ := parsed.RunWithFeatureFlags(context.Background(), variablesMap, store, numscriptFeatureFlags)

	// Propagate determinism violations — these are not transient errors.
	// We check the store directly because the numscript interpreter wraps store
	// errors without Unwrap(), making errors.As unusable.
	if store.nonDeterministic != nil {
		return nil, store.nonDeterministic
	}

	// Collect volume keys from balance queries (sources) with the real ledgerID
	result := make(map[data.VolumeKey]struct{}, len(store.queried))
	for key := range store.queried {
		key.LedgerID = ledgerID
		result[key] = struct{}{}
	}

	// Also collect volume keys from postings (both sources and destinations).
	// GetBalances only captures source accounts; destinations come from postings.
	for _, posting := range execResult.Postings {
		result[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: posting.Source},
			Asset:      posting.Asset,
		}] = struct{}{}
		result[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: posting.Destination},
			Asset:      posting.Asset,
		}] = struct{}{}
	}

	return result, nil
}
