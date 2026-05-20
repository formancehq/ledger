package numscript

import (
	"context"
	"maps"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
)

// MaxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account.
var MaxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)

// DiscoveryResult holds the results of Numscript dependency discovery.
// SourceVolumes contains accounts queried via GetBalances (posting sources).
// DestinationVolumes contains accounts that only appear as posting destinations.
// WrittenMetadata contains account metadata keys written via set_account_meta.
type DiscoveryResult struct {
	SourceVolumes      map[domain.VolumeKey]struct{}
	DestinationVolumes map[domain.VolumeKey]struct{}
	Metadata           map[domain.MetadataKey]struct{}
	WrittenMetadata    map[domain.MetadataKey]struct{}
}

// discoveryStore implements numscript.Store to discover which accounts/assets
// a Numscript script queries. It returns infinite balances so the script
// executes fully, and records every account/asset pair queried via GetBalances.
//
// Determinism constraint: GetBalances may be called at most once. A second call
// indicates a non-deterministic script (e.g., mid-script balance queries) which
// cannot be reliably preloaded. The violation is recorded in the
// nonDeterministic field and checked after execution.
//
// meta() calls are forbidden: GetAccountsMetadata returns ErrMetaNotSupported.
//
// This is a temporary workaround until the Numscript library implements static
// analysis of required inputs (see docs/drafts/numscript/numscript-static-inputs-rfc.md).
type discoveryStore struct {
	queriedVolumes   map[domain.VolumeKey]struct{}
	balancesCalled   bool
	nonDeterministic *ErrNonDeterministicScript
	metaCalled       bool
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

func (s *discoveryStore) GetAccountsMetadata(_ context.Context, _ numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	s.metaCalled = true

	return nil, ErrMetaNotSupported
}

// DiscoverNumscriptDependencies runs a Numscript script with a discovery store that
// returns infinite balances, solely to discover which accounts/assets the script
// queries. The returned keys have their LedgerID set to the provided value.
//
// Scripts must be deterministic: GetBalances may be called at most once. If a
// script calls it more than once (e.g., via mid-script balance queries),
// ErrNonDeterministicScript is returned.
//
// Scripts using meta() are rejected with ErrMetaNotSupported.
//
// Other execution errors are ignored: the discovery store returns infinite balances
// which may cause the script to follow different code paths. We collect whatever
// accounts were queried before the error occurred.
//
// Known limitation: with infinite balances, `oneof` may only query the first source.
func DiscoverNumscriptDependencies(cache *NumscriptCache, script string, vars map[string]string, ledgerID uint32) (*DiscoveryResult, error) {
	parsed, err := cache.GetOrParse(script)
	if err != nil {
		return nil, err
	}

	variablesMap := make(numscriptlib.VariablesMap, len(vars))
	maps.Copy(variablesMap, vars)

	store := &discoveryStore{
		queriedVolumes: make(map[domain.VolumeKey]struct{}),
	}

	// Run the script. The discovery store captures source accounts via GetBalances
	// and we extract destinations from the resulting postings.
	// Experimental features are declared directly in scripts via #![feature "..."].
	execResult, _ := SafeRun(parsed, context.Background(), variablesMap, store)

	// Reject scripts that use meta() — cannot preload dynamic accounts.
	if store.metaCalled {
		return nil, ErrMetaNotSupported
	}

	// Propagate determinism violations — these are not transient errors.
	// We check the store directly because the numscript interpreter wraps store
	// errors without Unwrap(), making errors.As unusable.
	if store.nonDeterministic != nil {
		return nil, store.nonDeterministic
	}

	// Collect source volume keys from balance queries with the real ledger ID
	sourceVolumes := make(map[domain.VolumeKey]struct{}, len(store.queriedVolumes))
	for key := range store.queriedVolumes {
		key.LedgerID = ledgerID
		sourceVolumes[key] = struct{}{}
	}

	// Also collect volume keys from postings: sources go into sourceVolumes,
	// destinations go into destinationVolumes.
	var destinationVolumes map[domain.VolumeKey]struct{}

	if len(execResult.Postings) > 0 {
		for _, posting := range execResult.Postings {
			sourceVolumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: posting.Source},
				Asset:      posting.Asset,
			}] = struct{}{}

			if destinationVolumes == nil {
				destinationVolumes = make(map[domain.VolumeKey]struct{})
			}

			destinationVolumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: posting.Destination},
				Asset:      posting.Asset,
			}] = struct{}{}
		}
	}

	// Collect account metadata keys written via set_account_meta.
	var writtenMetadata map[domain.MetadataKey]struct{}
	if len(execResult.AccountsMetadata) > 0 {
		writtenMetadata = make(map[domain.MetadataKey]struct{})
		for account, acctMeta := range execResult.AccountsMetadata {
			for key := range acctMeta {
				writtenMetadata[domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
					Key:        key,
				}] = struct{}{}
			}
		}
	}

	return &DiscoveryResult{
		SourceVolumes:      sourceVolumes,
		DestinationVolumes: destinationVolumes,
		WrittenMetadata:    writtenMetadata,
	}, nil
}
