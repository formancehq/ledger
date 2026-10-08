package processing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
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
	// compiledProgram/compiledProgramHash/compiledVars/compiledScriptHash are
	// the Numscript VM artifact admission compiled on the leader's parallel
	// path (from OrderTechnical, staged like the hash above), in one of the
	// shapes classifyCompiledArtifact accepts. Execution runs the bytecode on
	// the VM, the only engine: the committed artifact when this binary can
	// use it — the committed bytes by value; bytes proven by
	// compiledProgramHash to be the ones admission compiled by reference,
	// from this node's apply-side cache or from its own compile of the text —
	// and otherwise program and vars derived from the script text with this
	// binary's own library (numscript.SafeExecCommitted): an absent artifact
	// (audit replay), a bytecode version this library cannot read, or a
	// reference its compiler does not reproduce, the last two being a replica
	// on another library version during a rolling upgrade. The outcome is a
	// function of the committed entry and the running binary alone, never of
	// this node's cache (invariant #2); across library versions it rests on
	// the library keeping script semantics stable. A present program this
	// library reads but cannot decode or verify — an invalid header, corrupt
	// bytes — fails the order loudly.
	compiledProgram     []byte
	compiledProgramHash []byte
	compiledVars        []byte
	compiledScriptHash  []byte
	// compileMissing marks the store checker's audit replay, whose orders
	// never carry compiled code, so a missing artifact there is expected (see
	// RequestProcessor.CompileMissingNumscript).
	compileMissing bool
}

// compiledArtifactShape is how a committed scripted order carries its VM
// artifact — see OrderTechnical.compiled_program in raft_cmd.proto.
type compiledArtifactShape int

const (
	// artifactAbsent: none of the four fields. The store checker's audit
	// replay, whose orders keep only business fields; an admission bug
	// anywhere else.
	artifactAbsent compiledArtifactShape = iota
	// artifactByValue: program bytes, vars and script hash.
	artifactByValue
	// artifactByReference: program hash in place of the bytes, vars and
	// script hash — admission had already sent the bytes on an earlier order
	// of the script.
	artifactByReference
)

// classifyCompiledArtifact maps the four technical fields to the shape
// admission produced, or fails loudly (ErrNumscriptRuntime, invariant #7) on
// any combination it never does — both the program and its hash, either one
// without vars or script hash, vars or script hash alone, a program hash of
// the wrong length. It runs before any cache access and reads nothing but the
// committed fields, so a corrupt shape fails identically on every replica
// whatever its cache holds (invariant #2). The script hash's own length is
// left to the binding check against the resolved text, which rejects any
// value that is not the exact 16-byte hash.
func classifyCompiledArtifact(program, programHash, vars, scriptHash []byte) (compiledArtifactShape, domain.SerializableError) {
	hasProgram, hasProgramHash := len(program) > 0, len(programHash) > 0
	hasVars, hasScriptHash := len(vars) > 0, len(scriptHash) > 0

	switch {
	case !hasProgram && !hasProgramHash && !hasVars && !hasScriptHash:
		return artifactAbsent, nil
	case hasProgram && !hasProgramHash && hasVars && hasScriptHash:
		return artifactByValue, nil
	case !hasProgram && len(programHash) == len([16]byte{}) && hasVars && hasScriptHash:
		return artifactByReference, nil
	}

	return artifactAbsent, &domain.ErrNumscriptRuntime{
		Detail: fmt.Sprintf("compiled numscript artifact is partial: program=%t programHash=%d bytes vars=%t scriptHash=%t",
			hasProgram, len(programHash), hasVars, hasScriptHash),
	}
}

func (p *numscriptPostingProducer) produce(s Scope, ledgerName string, order *raftcmdpb.CreateTransactionOrder, script *commonpb.Script) (*produceResult, domain.SerializableError) {
	if script == nil || script.GetPlain() == "" {
		return nil, domain.ErrScriptRequired
	}

	// Hashed once: the stale-inputs parse below is keyed by it, and the
	// artifact binding check further down compares it to the committed hash.
	scriptHash := numscript.HashScript(script.GetPlain())

	// The artifact arrives by value, by reference, or not at all; every other
	// combination of the four fields is a corrupt state admission never
	// produces and fails loudly here, before any cache access and before the stale-inputs
	// re-resolution (which would otherwise report a malformed artifact as a
	// retryable stale input), so it fails identically on every replica
	// (invariants #2, #7).
	shape, shapeErr := classifyCompiledArtifact(p.compiledProgram, p.compiledProgramHash, p.compiledVars, p.compiledScriptHash)
	if shapeErr != nil {
		return nil, shapeErr
	}

	// The artifact is bound to the exact text admission compiled. The text
	// resolved here cannot differ — inline scripts travel in the order, exact
	// library versions are immutable, and an advanced "latest" was
	// stale-rejected before the producer ran — so a mismatch is a "should not
	// happen" surfaced loudly (invariant #7), never executing the wrong program.
	// An absent artifact has no hash to bind; its recompile below keys the
	// cache by the resolved text's own hash.
	if shape != artifactAbsent && !bytes.Equal(scriptHash[:], p.compiledScriptHash) {
		return nil, &domain.ErrNumscriptRuntime{
			Detail: "compiled numscript artifact does not match the resolved script text",
		}
	}

	// The artifact's headers are inspected here for the same reason the shape
	// is: a half whose header does not parse is corrupt, whatever bytecode
	// version the other half carries, and must fail loudly before the
	// stale-inputs re-resolution below can report the order as a retryable
	// stale input instead (numscript.CommittedArtifact.CheckHeaders reads only
	// the committed bytes, no state). classifyCompiledArtifact checked the
	// program hash is 16 bytes, which the conversion relies on.
	var artifact numscript.CommittedArtifact
	if shape != artifactAbsent {
		artifact = numscript.CommittedArtifact{Program: p.compiledProgram, Vars: p.compiledVars}
		if shape == artifactByReference {
			artifact.ProgramHash = [16]byte(p.compiledProgramHash)
		}

		if headerErr := artifact.CheckHeaders(); headerErr != nil {
			return nil, headerErr
		}
	}

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
		// Parse the script (uses cache to avoid re-parsing): dependency
		// re-resolution walks the AST; execution below runs the compiled artifact.
		parsed, err := p.cache.GetOrParseHashed(scriptHash, script.GetPlain())
		if err != nil {
			return nil, err
		}

		vars := make(numscriptlib.VariablesMap)
		maps.Copy(vars, script.GetVars())

		valueSource := &scopeValueSource{store: s, ledgerName: ledgerName}
		recording := numscript.NewRecordingStore(numscript.NewStore(valueSource, order.GetForce()), order.GetForce())

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
			// arrive here as a domain.SerializableError whose typed error survives the
			// numscript library's error path (the library's QueryBalanceError /
			// QueryMetadataError implement Unwrap, and convertNumscriptError
			// returns the Describable as-is). Softening this to retryable stale
			// would hide the bug and spin the client in an infinite re-admit loop
			// against the same missing declaration — surface it loudly (invariant
			// #7). Matched by domain Reason via domain.CoverageContractViolation
			// so this file need not import internal/infra/state (which imports
			// processing — import cycle).
			//
			// Accepted limitation (EN-1406): when the resolved dependency set is a
			// function of mutable metadata (e.g. `account $src = meta(@cfg,"acct")`)
			// and that metadata changed between admission and apply, re-resolution
			// derives a different account whose balance was never declared, so this
			// coverage miss is really a stale-input shift that a re-admission would
			// fix — yet it is classified fatal (KindInternal) rather than retryable
			// STALE. We keep it fatal on purpose: at the miss point we cannot
			// cheaply distinguish a value-derived shift (should be retryable) from a
			// genuine static under-declaration / preload-construction bug (must stay
			// fatal per invariant #7, else the client spins re-admitting the same
			// missing declaration). Downgrading only ErrCoverageMiss to stale would
			// close the value-shift case but risks masking the latter as an infinite
			// retry loop, so that refinement is deferred to an explicit design call.
			// Return the extracted violation rather than resolveErr: today
			// convertNumscriptError has already flattened the chain to the bare
			// Describable, so the two are the same value — but that flattening
			// lives two packages away and nothing pins it here. Returning what
			// the discriminator found keeps the reason intact regardless.
			if violation := domain.CoverageContractViolation(resolveErr); violation != nil {
				return nil, violation
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

	// Execute the script on the VM, the only execution engine. When Force is
	// true, the store returns unlimited balances to bypass balance checks.
	vmStore := numscript.NewVMStore(&scopeValueSource{store: s, ledgerName: ledgerName}, order.GetForce())

	var (
		result  numscriptlib.ExecutionResult
		execErr domain.SerializableError
	)

	switch shape {
	case artifactByValue, artifactByReference:
		// The committed artifact runs when this binary can use it, and the
		// order is derived from the script text with this binary's own
		// library otherwise (see SafeExecCommitted). By value that means the
		// committed bytes and vars, as long as their bytecode version is one
		// this library reads. By reference — admission already sent the bytes
		// on an earlier order of the script (CompiledScript.AlreadyCompiled:
		// its own compile cache's memory, not a claim about this node's) — it
		// means bytes with the committed hash: this node's apply-side cache in
		// steady state (applying that earlier order warmed every replica), or
		// its own compile of the text after a restart, an LRU eviction, or a
		// late join, when that compile reproduces the hash. A version this
		// library cannot read, or a compile that does not reproduce the hash,
		// is a replica on another library version (a rolling upgrade), not a
		// defect: program and vars are then derived from the text. Within one
		// library version the outcome is therefore independent of this node's
		// cache (invariant #2); across versions it rests on the library
		// keeping script semantics stable, as audit replay already does.
		result, execErr = numscript.SafeExecCommitted(p.cache, scriptHash, artifact, script.GetPlain(), script.GetVars(), vmStore)
	case artifactAbsent:
		// The artifact is derivable from the script text, so an absent one is
		// recompiled here exactly as admission compiles it: it costs a compile
		// and, under the same bundled library, never changes the outcome
		// (history from a library with different execution semantics can
		// replay differently, see numscript.CompileForReplay). Re-running an
		// audited order, which never carries compiled code, always gets here.
		// Anywhere else an absent artifact is an admission bug — admission
		// binds one to every scripted order it proposes, and one it forwards
		// without is marked preload_unavailable and rejected before reaching
		// here — so it is flagged under Antithesis (invariant #7).
		if !p.compileMissing {
			assert.Unreachable("scripted order reached FSM apply without its compiled numscript artifact", map[string]any{
				"ledger": ledgerName,
			})
		}

		result, execErr = numscript.SafeExecFromText(p.cache, script.GetPlain(), script.GetVars(), vmStore)
	default:
		// classifyCompiledArtifact returns only the three shapes above. A
		// fourth value is a programming error and must surface rather than
		// fall through to an empty result (invariant #7).
		return nil, &domain.ErrNumscriptRuntime{
			Detail: fmt.Sprintf("compiled numscript artifact: unknown shape %d", shape),
		}
	}

	if execErr != nil {
		return nil, execErr
	}

	// Convert numscript postings to commonpb postings and update buffer
	postings := make([]*commonpb.Posting, len(result.Postings))

	var (
		scratch    uint256.Int // reused across all postings
		sum        uint256.Int
		u256Amount uint256.Int
	)

	for i, posting := range result.Postings {
		// Authoritative rejection of scope-qualified postings. Color IS modelled —
		// posting.Color flows into the commonpb.Posting and NewVolumeKey below, so
		// a colored posting materialises its own segregated volume bucket. Scope is
		// NOT modelled: building a commonpb.Posting would silently drop the scope
		// qualifier and collapse onto the unscoped volume — a silent semantic loss.
		// Reject deterministically so every node produces the same definitive
		// failure. Mirrors admission's discover-side rejection.
		if posting.SourceScope != "" || posting.DestinationScope != "" {
			return nil, domain.ErrScopedBalanceUnsupported
		}

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
			return nil, domain.StoreFailure(
				fmt.Sprintf("source volume %s/%s color=%q", posting.Source, posting.Asset, posting.Color), err)
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
			return nil, domain.StoreFailure(
				fmt.Sprintf("destination volume %s/%s color=%q", posting.Destination, posting.Asset, posting.Color), err)
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
		for _, row := range result.AccountsMetadata {
			// Ledger account metadata has no scope dimension; a scoped write would
			// silently collapse onto the unscoped key. Reject deterministically,
			// mirroring the posting and discover-side scope rejections.
			if row.Scope != "" {
				return nil, domain.ErrScopedBalanceUnsupported
			}

			if err := domain.ValidateMetadataKey(row.Key); err != nil {
				return nil, &domain.ErrAccountValidation{Account: row.Account, Cause: err}
			}

			// The library renders metadata values to their stored string form.
			value := row.Value

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

	// Transaction metadata arrives already rendered to strings by the library.
	var txMeta map[string]*commonpb.MetadataValue
	if len(result.Metadata) > 0 {
		txMeta = make(map[string]*commonpb.MetadataValue, len(result.Metadata))
		// A validation failure becomes hash-chained audit state. Select the
		// first invalid key canonically on every replica.
		keys := slices.Sorted(maps.Keys(result.Metadata))
		for _, key := range keys {
			value := result.Metadata[key]
			if err := domain.ValidateMetadataKey(key); err != nil {
				return nil, err
			}

			if err := domain.ValidateMetadataString(value); err != nil {
				return nil, &domain.ErrMetadataKeyValidation{Key: key, Cause: err}
			}

			txMeta[key] = commonpb.NewStringValue(value)
		}
	}

	return &produceResult{
		Postings:            postings,
		TransactionMetadata: txMeta,
		AccountsMetadata:    accountsMeta,
	}, nil
}

// scopeValueSource reads balances and metadata through the FSM apply Scope.
// Every read passes through the coverage gate (invariant #9) and touches only
// preloaded cache values, never Pebble (invariant #3). It backs both the
// force-free execution store and the FSM-time stale-inputs re-resolution.
type scopeValueSource struct {
	store      Scope
	ledgerName string
}

func (s *scopeValueSource) Balance(account, asset, color string) (*big.Int, error) {
	// #1560 (EN-1406) resolves dependencies through the upstream
	// ResolveDependencies API, which threads color: a colored balance read
	// resolves its own segregated (account, asset, color) bucket through the
	// coverage-gated Scope (scope-qualified reads are still rejected earlier via
	// domain.ErrScopedBalanceUnsupported).
	volumeKey := domain.NewVolumeKey(s.ledgerName, account, asset, color)

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
