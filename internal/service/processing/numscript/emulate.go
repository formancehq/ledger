package numscript

import (
	"context"
	"maps"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	numscriptlib "github.com/formancehq/numscript"
)

// DiscoveryResult holds the results of Numscript dependency discovery.
// It contains both the volume keys and metadata keys that a script queries.
type DiscoveryResult struct {
	Volumes  map[domain.VolumeKey]struct{}
	Metadata map[domain.MetadataKey]struct{}
}

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
// analysis of required inputs (see docs/drafts/numscript/numscript-static-inputs-rfc.md).
type discoveryStore struct {
	queriedVolumes   map[domain.VolumeKey]struct{}
	queriedMetadata  map[domain.MetadataKey]struct{}
	balancesCalled   bool
	metadataCalled   bool
	nonDeterministic *ErrNonDeterministicScript
}

func (s *discoveryStore) GetBalances(_ context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	if s.balancesCalled {
		s.nonDeterministic = &ErrNonDeterministicScript{Method: "GetBalances"}
		return nil, s.nonDeterministic
	}
	s.balancesCalled = true

	balances := make(numscriptlib.Balances, len(query))
	for account, assets := range query {
		accountBalance := make(numscriptlib.AccountBalance, len(assets))
		balances[account] = accountBalance
		for _, asset := range assets {
			s.queriedVolumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{Account: account},
				Asset:      asset,
			}] = struct{}{}
			accountBalance[asset] = new(big.Int).Set(MaxForceBalance)
		}
	}
	return balances, nil
}

func (s *discoveryStore) GetAccountsMetadata(_ context.Context, query numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	if s.metadataCalled {
		s.nonDeterministic = &ErrNonDeterministicScript{Method: "GetAccountsMetadata"}
		return nil, s.nonDeterministic
	}
	s.metadataCalled = true

	result := make(numscriptlib.AccountsMetadata, len(query))
	for account, keys := range query {
		result[account] = make(numscriptlib.AccountMetadata)
		for _, key := range keys {
			s.queriedMetadata[domain.MetadataKey{
				AccountKey: domain.AccountKey{Account: account},
				Key:        key,
			}] = struct{}{}
		}
	}
	return result, nil
}

// DiscoverNumscriptDependencies runs a Numscript script with a discovery store that
// returns infinite balances, solely to discover which accounts/assets and metadata keys
// the script queries. The returned keys have their Ledger set to the provided value.
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
func DiscoverNumscriptDependencies(script string, vars map[string]string, ledger string) (*DiscoveryResult, error) {
	parsed := numscriptlib.Parse(script)
	if errs := parsed.GetParsingErrors(); len(errs) > 0 {
		return nil, &ErrNumscriptParse{
			Details: numscriptlib.ParseErrorsToString(errs, parsed.GetSource()),
		}
	}

	variablesMap := make(numscriptlib.VariablesMap, len(vars))
	maps.Copy(variablesMap, vars)

	store := &discoveryStore{
		queriedVolumes:  make(map[domain.VolumeKey]struct{}),
		queriedMetadata: make(map[domain.MetadataKey]struct{}),
	}

	// Run the script. The discovery store captures source accounts via GetBalances
	// and we extract destinations from the resulting postings.
	execResult, _ := parsed.RunWithFeatureFlags(context.Background(), variablesMap, store, FeatureFlags)

	// Propagate determinism violations — these are not transient errors.
	// We check the store directly because the numscript interpreter wraps store
	// errors without Unwrap(), making errors.As unusable.
	if store.nonDeterministic != nil {
		return nil, store.nonDeterministic
	}

	// Collect volume keys from balance queries (sources) with the real ledger name
	volumes := make(map[domain.VolumeKey]struct{}, len(store.queriedVolumes))
	for key := range store.queriedVolumes {
		key.Ledger = ledger
		volumes[key] = struct{}{}
	}

	// Also collect volume keys from postings (both sources and destinations).
	// GetBalances only captures source accounts; destinations come from postings.
	for _, posting := range execResult.Postings {
		volumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.Source},
			Asset:      posting.Asset,
		}] = struct{}{}
		volumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.Destination},
			Asset:      posting.Asset,
		}] = struct{}{}
	}

	// Collect metadata keys with the real ledger name
	metadata := make(map[domain.MetadataKey]struct{}, len(store.queriedMetadata))
	for key := range store.queriedMetadata {
		key.Ledger = ledger
		metadata[key] = struct{}{}
	}

	return &DiscoveryResult{
		Volumes:  volumes,
		Metadata: metadata,
	}, nil
}
