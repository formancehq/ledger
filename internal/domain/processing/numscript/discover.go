package numscript

import (
	"context"
	"maps"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// DiscoveryResult holds the Ledger-shaped dependency set resolved from a
// Numscript script via the upstream ResolveDependencies API, plus the
// stale-inputs hash binding the balance/metadata values that resolution read.
//
// ReadVolumes are (account, asset) balances the script's resolution consulted
// (bounded sources, capped/allotment amounts, balance()/overdraft() in vars or
// selectors). WriteVolumes are (account, asset) pairs the script credits or
// debits (sources — including unbounded ones — and destinations). A key can be
// in both. Admission preloads the union so the FSM apply path can read/mutate
// every touched volume from cache.
//
// ReadMetadata are (account, key) metadata entries meta() consulted;
// WriteMetadata are entries set_account_meta writes. Only reads must be
// preloaded for the FSM; writes are tracked for coverage of the metadata the
// apply path will mutate.
//
// InputsHash is nil when resolution read no balance or metadata (a fully
// static script): there are no inputs to bind, so no stale check is needed.
type DiscoveryResult struct {
	ReadVolumes   map[domain.VolumeKey]struct{}
	WriteVolumes  map[domain.VolumeKey]struct{}
	ReadMetadata  map[domain.MetadataKey]struct{}
	WriteMetadata map[domain.MetadataKey]struct{}
	InputsHash    []byte
}

// DiscoverNumscriptDependencies statically resolves the accounts, assets and
// metadata a Numscript script reads and writes, resolving var origins and
// posting selectors against the given ValueSource. It replaces the previous
// emulation-based discovery: oneof branches are fully explored, multi-send is
// supported, and meta() is honoured (its reads become metadata dependencies).
//
// The ValueSource must expose admission-time state (Pebble snapshot). Its reads
// are recorded and hashed into DiscoveryResult.InputsHash so the FSM can detect
// inputs that changed before apply (domain.ErrStaleInputsResolution).
//
// force mirrors the transaction's force flag: with force the resolver sees
// unlimited balances, so bounded sources still resolve but no real balance is
// consulted.
func DiscoverNumscriptDependencies(
	cache *NumscriptCache,
	script string,
	vars map[string]string,
	ledgerName string,
	source ValueSource,
	force bool,
) (*DiscoveryResult, error) {
	parsed, parseErr := cache.GetOrParse(script)
	if parseErr != nil {
		return nil, parseErr
	}

	variablesMap := make(numscriptlib.VariablesMap, len(vars))
	maps.Copy(variablesMap, vars)

	recording := NewRecordingStore(NewStore(source, force))

	resolved, err := parsed.ResolveDependencies(context.Background(), variablesMap, recording)
	if err != nil {
		return nil, convertNumscriptError(err)
	}

	result := &DiscoveryResult{
		ReadVolumes:   make(map[domain.VolumeKey]struct{}, len(resolved.AccountsReads)),
		WriteVolumes:  make(map[domain.VolumeKey]struct{}, len(resolved.AccountsWrites)),
		ReadMetadata:  make(map[domain.MetadataKey]struct{}, len(resolved.MetaReads)),
		WriteMetadata: make(map[domain.MetadataKey]struct{}, len(resolved.MetaWrites)),
		InputsHash:    recording.Hash(),
	}

	// Ledger volumes are keyed by (ledger, account, asset) only — color and
	// scope are not modelled, so distinct-color dependencies on the same
	// (account, asset) collapse to the same preload key. That is correct: the
	// preload set is a set of volumes to load, and the color is irrelevant to
	// which Pebble/cache entry is touched.
	for dep := range resolved.AccountsReads {
		result.ReadVolumes[domain.NewVolumeKey(ledgerName, dep.Account, dep.Asset)] = struct{}{}
	}

	for dep := range resolved.AccountsWrites {
		result.WriteVolumes[domain.NewVolumeKey(ledgerName, dep.Account, dep.Asset)] = struct{}{}
	}

	for dep := range resolved.MetaReads {
		result.ReadMetadata[domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: dep.Account},
			Key:        dep.Key,
		}] = struct{}{}
	}

	for dep := range resolved.MetaWrites {
		result.WriteMetadata[domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: dep.Account},
			Key:        dep.Key,
		}] = struct{}{}
	}

	return result, nil
}
