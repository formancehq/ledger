package processing

import (
	"bytes"
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
	ledgerName string
	assetCache map[string]cachedAssetPrecision
	// inputsResolutionHash is the admission-derived hash for this order (from
	// OrderTechnical, staged on Context by the dispatcher) — the baseline the
	// stale-inputs check re-resolves against. Empty means nothing to check.
	inputsResolutionHash []byte
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

	// Stale-inputs check: admission bound the balance/metadata values its
	// dependency resolution read into OrderTechnical.inputs_resolution_hash
	// (staged on p.inputsResolutionHash by the dispatcher). Re-resolve
	// here against the coverage-gated Scope (preloaded cache values only — no
	// Pebble reads, invariant #3) and compare. A mismatch means an input value
	// changed between admission and apply, so the preloaded key set may be
	// wrong; reject with the retryable ErrStaleInputsResolution so the client
	// re-admits against the new values. An empty stored hash means admission's
	// resolution read nothing to bind (fully static script) — nothing to check.
	if expected := p.inputsResolutionHash; len(expected) > 0 {
		valueSource := &scopeValueSource{store: s, ledgerName: ledgerName}
		recording := numscript.NewRecordingStore(numscript.NewStore(valueSource, order.GetForce()))

		// SafeResolveDependencies recovers panics from the numscript library so a
		// crafted input can never escape onto the deterministic Raft apply loop
		// (which has no recover of its own) — an escaped panic would crash and
		// could diverge nodes. Not every resolve error is a stale input, so the
		// three failure classes are triaged below.
		if _, resolveErr := numscript.SafeResolveDependencies(parsed, context.Background(), vars, recording); resolveErr != nil {
			// (1) A recovered panic is a "should not happen" (invariant #7) and
			// must surface loudly, NOT be softened to stale.
			if numscript.IsPanic(resolveErr) {
				return nil, resolveErr
			}

			// (2) A coverage-contract violation is an admission BUG: the
			// resolution derived a key admission never declared, so the gated
			// Scope refused the read (*state.ErrCoverageMiss) or the plan was
			// structurally inconsistent (*domain.ErrInvalidExecutionPlan). Both
			// arrive here as a domain.Describable whose typed error survives the
			// numscript library's error path (the library's QueryBalanceError /
			// QueryMetadataError implement Unwrap, and convertNumscriptError
			// returns the Describable as-is). Softening this to retryable stale
			// would hide the bug and spin the client in an infinite re-admit loop
			// against the same missing declaration — surface it loudly (invariant
			// #7). Matched by domain Reason so this file need not import
			// internal/infra/state (which imports processing — import cycle).
			if isCoverageContractViolation(resolveErr) {
				return nil, resolveErr
			}

			// (3) Otherwise it is a genuine input-shift (a var origin now points
			// at a missing/changed value) on a script admission already resolved,
			// not a client script bug — surface it as stale so the client
			// re-admits against fresh state.
			return nil, domain.ErrStaleInputsResolution
		}

		if !bytes.Equal(expected, recording.Hash()) {
			return nil, domain.ErrStaleInputsResolution
		}
	}

	// Execute the script (experimental features are declared directly in scripts).
	// When Force is true, the store returns unlimited balances to bypass balance checks.
	storeAdapter := numscript.NewStore(&scopeValueSource{store: s, ledgerName: ledgerName}, order.GetForce())
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
	var accountsMeta map[string]map[string]*commonpb.MetadataValue
	if len(result.AccountsMetadata) > 0 {
		accountsMeta = make(map[string]map[string]*commonpb.MetadataValue, len(result.AccountsMetadata))
		for _, row := range result.AccountsMetadata {
			if err := domain.ValidateMetadataKey(row.Key); err != nil {
				return nil, &domain.ErrAccountValidation{Account: row.Account, Cause: err}
			}

			value, convErr := numscript.ValueToString(row.Value)
			if convErr != nil {
				// A Numscript value that fails to serialise is a library-level
				// impossibility, not a client error — surface it loudly.
				return nil, &domain.ErrNumscriptRuntime{
					Detail: fmt.Sprintf("serialising account metadata %s/%s: %v", row.Account, row.Key, convErr),
				}
			}

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

			stringValue, convErr := numscript.ValueToString(value)
			if convErr != nil {
				return nil, &domain.ErrNumscriptRuntime{
					Detail: fmt.Sprintf("serialising transaction metadata %s: %v", key, convErr),
				}
			}

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

// isCoverageContractViolation reports whether err is an admission-contract
// violation surfaced by the coverage-gated Scope during apply-time re-resolution
// — a *state.ErrCoverageMiss (a key admission never declared) or a
// *domain.ErrInvalidExecutionPlan (a structurally inconsistent plan). Both are
// "should not happen" bugs (invariant #7) that must surface loudly rather than
// be softened to the retryable ErrStaleInputsResolution.
//
// It matches on the stable domain Reason string rather than the concrete type so
// this package need not import internal/infra/state (state imports processing —
// an import cycle). Every Describable in the chain is inspected because the
// numscript library wraps the store error (QueryBalanceError / QueryMetadataError
// both implement Unwrap), so errors.As can reach the underlying typed error.
func isCoverageContractViolation(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		describable, ok := e.(domain.Describable) //nolint:errorlint // deliberate per-node check; the loop unwraps the chain itself
		if !ok {
			continue
		}

		switch describable.Reason() {
		case domain.ErrReasonCoverageMiss, domain.ErrReasonInvalidExecutionPlan:
			return true
		}
	}

	return false
}

// scopeValueSource reads balances and metadata through the FSM apply Scope.
// Every read passes through the coverage gate (invariant #9) and touches only
// preloaded cache values, never Pebble (invariant #3). It backs both the
// force-free execution store and the FSM-time stale-inputs re-resolution.
type scopeValueSource struct {
	store      Scope
	ledgerName string
}

func (s *scopeValueSource) Balance(account, asset string) (*big.Int, error) {
	volumeKey := domain.NewVolumeKey(s.ledgerName, account, asset)

	vol, err := readVolumeOrZero(s.store, volumeKey)
	if err != nil {
		return nil, err
	}

	if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil {
		return nil, &domain.ErrBalanceNotPreloaded{Account: account, Asset: asset}
	}

	var inputVal, outputVal uint256.Int
	vol.GetInput().IntoUint256(&inputVal)
	vol.GetOutput().IntoUint256(&outputVal)

	// Convert to *big.Int at the numscript boundary (numscript uses *big.Int).
	return new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig()), nil
}

func (s *scopeValueSource) Metadata(account, key string) (string, bool, error) {
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{
			LedgerName: s.ledgerName,
			Account:    account,
		},
		Key: key,
	}

	valueReader, err := s.store.AccountMetadata().Get(metaKey)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return "", false, err
	}

	if valueReader == nil {
		// Absent key: Scope returns ErrNotFound → nil reader above.
		return "", false, nil
	}

	// Present key. Numscript sees the verbatim client write — declared_type is an
	// index hint only and MUST NOT influence script behaviour. A previous version
	// coerced "030" under a UINT64 declaration to "30" here, which broke the
	// lossless contract and let a retype silently change transaction outcomes.
	//
	// Presence is driven ONLY by nil-ness: an empty string is a valid stored
	// metadata value, and MetadataValueToString returns "" for both a real
	// StringValue("") and an untyped/nil value. Returning present=false on
	// str=="" would make a valid meta() read of an empty string resolve as
	// absent, diverging from the admission-side admissionValueSource and
	// poisoning the resolution hash with the absent sentinel.
	return commonpb.MetadataValueToString(valueReader.Mutate()), true, nil
}
