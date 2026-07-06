package numscript

import (
	"context"
	"maps"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"
	"github.com/formancehq/numscript/accounts"

	"github.com/formancehq/ledger/v3/internal/domain"
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
// queries. The returned keys have their LedgerName set to the provided value.
//
// Scripts must be deterministic: GetBalances may be called at most once. If a
// script calls it more than once (e.g., via mid-script balance queries),
// ErrNonDeterministicScript is returned.
//
// Scripts using meta() are rejected with ErrMetaNotSupported.
//
// Other execution errors are returned so admission rejects scripts that cannot
// be emulated safely.
//
// Known limitation: with infinite balances, `oneof` may only query the first source.
func DiscoverNumscriptDependencies(cache *NumscriptCache, script string, vars map[string]string, ledgerName string) (*DiscoveryResult, error) {
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
	execResult, execErr := SafeRun(parsed, context.Background(), variablesMap, store)

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

	if execErr != nil {
		return nil, execErr
	}

	// Collect source volume keys from balance queries with the real ledger ID
	sourceVolumes := make(map[domain.VolumeKey]struct{}, len(store.queriedVolumes))
	for key := range store.queriedVolumes {
		key.LedgerName = ledgerName
		sourceVolumes[key] = struct{}{}
	}

	// Also collect volume keys from postings: sources go into sourceVolumes,
	// destinations go into destinationVolumes.
	var destinationVolumes map[domain.VolumeKey]struct{}

	if len(execResult.Postings) > 0 {
		for _, posting := range execResult.Postings {
			sourceVolumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: posting.Source},
				Asset:      posting.Asset,
			}] = struct{}{}

			if destinationVolumes == nil {
				destinationVolumes = make(map[domain.VolumeKey]struct{})
			}

			destinationVolumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: posting.Destination},
				Asset:      posting.Asset,
			}] = struct{}{}
		}
	}

	// Emulation gap fill: the discovery store returns fake infinite (positive)
	// balances, so a balance-dependent amount can collapse to zero. In particular
	// overdraft(@acc, A) folds to zero against a positive balance, turning
	// `send overdraft(...)` into a zero-amount send that emits no posting. The
	// source account then appears in neither queriedVolumes nor the postings and
	// is missed entirely, causing "read of undeclared key" at execution
	// (formancehq/ledger#1500).
	//
	// GetInvolvedAccounts statically walks the AST and reports every account each
	// send touches — including `... allowing unbounded overdraft` sources —
	// regardless of the resolved amount. We union the statically-resolvable pairs
	// that emulation did not already classify as destinations into sourceVolumes.
	// Over-preloading is safe (it only locks an extra row); under-preloading is
	// the bug. Accounts already in destinationVolumes are skipped so the
	// source/destination partition is preserved for scripts emulation handles.
	//
	// Best-effort: GetInvolvedAccounts can legitimately error on experimental
	// constructs it does not model yet (e.g. asset scaling / colored sources).
	// The script already emulated successfully, so on error we keep the
	// emulation-only result rather than rejecting a transaction that works today.
	if involved, _, involvedErr := parsed.GetInvolvedAccounts(variablesMap); involvedErr == nil {
		for _, ia := range involved {
			account, okAccount := resolveInvolvedAccount(ia.AccountExpr)
			asset, okAsset := resolveInvolvedAsset(ia.AssetExpr)
			if !okAccount || !okAsset {
				continue
			}

			key := domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
				Asset:      asset,
			}
			if _, isDestination := destinationVolumes[key]; isDestination {
				continue
			}

			sourceVolumes[key] = struct{}{}
		}
	}

	// Collect account metadata keys written via set_account_meta.
	var writtenMetadata map[domain.MetadataKey]struct{}
	if len(execResult.AccountsMetadata) > 0 {
		writtenMetadata = make(map[domain.MetadataKey]struct{})
		for account, acctMeta := range execResult.AccountsMetadata {
			for key := range acctMeta {
				writtenMetadata[domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
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

// resolveInvolvedAccount folds an involved-account name expression to a concrete
// account string. Variables are already substituted (they are bound in
// variablesMap during GetInvolvedAccounts) and meta() is rejected earlier, so
// the only shapes that reach here are literals and concatenations of them.
// Returns false for anything it cannot statically resolve, so the caller skips
// it rather than preloading a wrong key.
func resolveInvolvedAccount(expr accounts.InvolvedAccountExpr) (string, bool) {
	switch e := expr.(type) {
	case accounts.AccountLiteral:
		return e.Account, true
	case accounts.StringLiteral:
		return e.String, true
	case accounts.NumberLiteral:
		if e.Amount == nil {
			return "", false
		}
		return e.Amount.String(), true
	case accounts.ConcatAccount:
		left, ok := resolveInvolvedAccount(e.Left)
		if !ok {
			return "", false
		}
		right, ok := resolveInvolvedAccount(e.Right)
		if !ok {
			return "", false
		}
		return left + right, true
	}
	return "", false
}

// resolveInvolvedAsset folds an involved-account asset expression to a concrete
// asset string. GetAsset is transparent, and the asset of a monetary produced by
// balance()/overdraft()/a monetary literal is its asset argument, so the whole
// chain (e.g. GetAsset{GetOverdraft{@credit, USD/2}}) collapses to the literal
// "USD/2". Returns false for shapes it cannot statically resolve.
func resolveInvolvedAsset(expr accounts.InvolvedAccountExpr) (string, bool) {
	switch e := expr.(type) {
	case accounts.AssetLiteral:
		return e.Asset, true
	case accounts.MakeMonetary:
		return resolveInvolvedAsset(e.Asset)
	case accounts.GetBalance:
		return resolveInvolvedAsset(e.Asset)
	case accounts.GetOverdraft:
		return resolveInvolvedAsset(e.Asset)
	case accounts.GetAsset:
		return resolveInvolvedAsset(e.Monetary)
	}
	return "", false
}
