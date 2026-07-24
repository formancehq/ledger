package numscript

import (
	"context"
	"maps"
	"math/big"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// DiscoveryResult holds the Ledger-shaped dependency set resolved from a
// Numscript script via the upstream ResolveDependencies API, plus the
// stale-inputs hash binding the balance/metadata values that resolution read.
//
// ReadVolumes are (account, asset, color) balances the script's resolution
// consulted (bounded sources, capped/allotment amounts, balance()/overdraft() in
// vars or selectors). WriteVolumes are (account, asset, color) keys the script
// credits or debits (sources — including unbounded ones — and destinations); a
// colored posting resolves its own segregated bucket. A key can be in both.
// Admission preloads the union so the FSM apply path can read/mutate every
// touched volume from cache.
//
// ReadMetadata are (account, key) metadata entries meta() consulted;
// WriteMetadata are entries set_account_meta writes. Only reads must be
// preloaded for the FSM; writes are tracked for coverage of the metadata the
// apply path will mutate.
//
// InputsHash is nil when resolution read no balance or metadata (a fully
// static script): there are no inputs to bind, so no stale check is needed.
//
// Effects capture what this script WOULD do to state, evaluated against the same
// source. They let a later order in the SAME atomic batch resolve against the
// state its predecessors will leave behind (EN-1406 P1-1): the FSM applies batch
// orders sequentially against a mutated WriteSet, so admission must resolve each
// order against pre-batch storage PLUS the accumulated effects of the orders
// ahead of it — otherwise a balance()/meta() that depends on an earlier order
// hashes stale and is rejected forever. NetBalanceDeltas is keyed by
// (ledger, account, asset, color) and holds (input−output) deltas — the same
// quantity balance() returns for that color bucket. MetadataWrites holds the raw
// values set_account_meta wrote.
type DiscoveryResult struct {
	ReadVolumes   map[domain.VolumeKey]struct{}
	WriteVolumes  map[domain.VolumeKey]struct{}
	ReadMetadata  map[domain.MetadataKey]struct{}
	WriteMetadata map[domain.MetadataKey]struct{}
	InputsHash    []byte

	NetBalanceDeltas map[domain.VolumeKey]*big.Int
	MetadataWrites   map[domain.MetadataKey]string
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

	resolved, err := SafeResolveDependencies(parsed, context.Background(), variablesMap, recording)
	if err != nil {
		return nil, &DependencyResolutionError{
			Cause:                err,
			MutableReadAttempted: recording.MutableReadAttempted(),
		}
	}

	result := &DiscoveryResult{
		ReadVolumes:   make(map[domain.VolumeKey]struct{}, len(resolved.AccountsReads)),
		WriteVolumes:  make(map[domain.VolumeKey]struct{}, len(resolved.AccountsWrites)),
		ReadMetadata:  make(map[domain.MetadataKey]struct{}, len(resolved.MetaReads)),
		WriteMetadata: make(map[domain.MetadataKey]struct{}, len(resolved.MetaWrites)),
		InputsHash:    recording.Hash(),
	}

	// Ledger volumes are keyed by (ledger, account, asset, color): color IS a
	// volume dimension, so a colored read dependency preloads its own segregated
	// bucket. Scope is not modelled — but a scope-qualified read is already
	// rejected by the Store during SafeResolveDependencies above, so no extra
	// guard is needed here.
	for dep := range resolved.AccountsReads {
		result.ReadVolumes[domain.NewVolumeKey(ledgerName, dep.Account, dep.Asset, dep.Color)] = struct{}{}
	}

	for dep := range resolved.AccountsWrites {
		// Reject scope-qualified WRITE dependencies. Writes are recorded without
		// touching the Store, so a scoped destination would otherwise be silently
		// collapsed onto the unscoped (account, asset) volume — a silent semantic
		// loss, since Ledger account volumes have no scope dimension. Color is
		// modelled, so a colored write preloads its own segregated bucket.
		if dep.Scope != "" {
			return nil, domain.ErrScopedBalanceUnsupported
		}
		result.WriteVolumes[domain.NewVolumeKey(ledgerName, dep.Account, dep.Asset, dep.Color)] = struct{}{}
	}

	for dep := range resolved.MetaReads {
		result.ReadMetadata[domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: dep.Account},
			Key:        dep.Key,
		}] = struct{}{}
	}

	for dep := range resolved.MetaWrites {
		// Reject scope-qualified metadata WRITE dependencies, same rationale as
		// scoped balance writes above — Ledger account metadata has no scope
		// dimension, so a scoped write would silently collapse onto the unscoped key.
		if dep.Scope != "" {
			return nil, domain.ErrScopedBalanceUnsupported
		}
		result.WriteMetadata[domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: dep.Account},
			Key:        dep.Key,
		}] = struct{}{}
	}

	// Compute this script's effects (net balance deltas + metadata writes) by
	// executing it against the same source. A later order in the same atomic
	// batch resolves against pre-batch storage plus these effects, mirroring the
	// FSM's sequential apply over a mutated WriteSet (EN-1406 P1-1). Executing
	// here uses the in-script feature flags (parsed.Run merges #![feature]) so it
	// matches the FSM run.
	//
	// Effects are best-effort: a runtime failure here (insufficient funds against
	// admission-time state, overflow, …) is NOT an admission rejection. Balance
	// enforcement is the FSM's authoritative job — admission's contract is to
	// preload the right keys and bind the resolution hash, both already done
	// above. If the script can't execute against the state visible now, it simply
	// contributes no intra-batch effects; the order is still proposed and the FSM
	// produces the definitive outcome (and the stale-inputs hash catches any
	// divergence between admission-time and apply-time state). Running the check
	// here as a hard reject would prematurely fail orders that the FSM would
	// accept once earlier same-batch or concurrent orders have moved balances.
	execResult, execErr := SafeRun(parsed, context.Background(), variablesMap, NewStore(source, force))
	if execErr != nil {
		// A recovered numscript-library panic is a "should not happen"
		// (CLAUDE.md invariant #7) and must surface loudly — exactly like the
		// SafeResolveDependencies path above and the FSM re-resolution
		// (processor_transaction_numscript.go). Only genuine runtime failures
		// (insufficient funds against admission-time state, overflow, …) stay
		// best-effort here: admission's contract is preload + resolution hash,
		// and the FSM produces the authoritative outcome.
		if IsPanic(execErr) {
			return nil, execErr
		}

		return result, nil
	}

	result.NetBalanceDeltas = make(map[domain.VolumeKey]*big.Int)
	for _, posting := range execResult.Postings {
		if posting.Amount == nil {
			continue
		}
		// balance = input − output. Source is debited (balance −amount), the
		// destination is credited (balance +amount). Color IS modelled, so the
		// delta lands on the posting's segregated (account, asset, color) bucket.
		srcKey := domain.NewVolumeKey(ledgerName, posting.Source, posting.Asset, posting.Color)
		addBalanceDelta(result.NetBalanceDeltas, srcKey, new(big.Int).Neg(posting.Amount))

		dstKey := domain.NewVolumeKey(ledgerName, posting.Destination, posting.Asset, posting.Color)
		addBalanceDelta(result.NetBalanceDeltas, dstKey, posting.Amount)
	}

	if len(execResult.AccountsMetadata) > 0 {
		result.MetadataWrites = make(map[domain.MetadataKey]string, len(execResult.AccountsMetadata))
		for _, row := range execResult.AccountsMetadata {
			value, convErr := ValueToString(row.Value)
			if convErr != nil {
				return nil, convertNumscriptError(convErr)
			}

			result.MetadataWrites[domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: row.Account},
				Key:        row.Key,
			}] = value
		}
	}

	return result, nil
}

// addBalanceDelta accumulates delta into m[key], allocating a fresh big.Int the
// first time so callers' amounts are never aliased.
func addBalanceDelta(m map[domain.VolumeKey]*big.Int, key domain.VolumeKey, delta *big.Int) {
	if existing, ok := m[key]; ok {
		existing.Add(existing, delta)

		return
	}

	m[key] = new(big.Int).Set(delta)
}
