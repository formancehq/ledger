package oracle

import (
	"context"
	"errors"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"

	"github.com/holiman/uint256"

	numscript "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// The model runs scripts on numscript's tree-walking interpreter while the
// server runs the compiled VM, so every committed script is a differential
// check of the two engines. Everything around the run — reference resolution,
// admission's classification of failures, the FSM's conversion of the result —
// mirrors the server step for step.

// NumscriptKey identifies one stored library version.
type NumscriptKey struct {
	Name    string
	Version string
}

type numscriptKeyComparer struct{}

func (numscriptKeyComparer) Compare(a, b NumscriptKey) int {
	if c := strings.Compare(a.Name, b.Name); c != 0 {
		return c
	}

	return strings.Compare(a.Version, b.Version)
}

func numscriptTerm(k NumscriptKey, content string) Digest {
	t := newTerm("NS")
	t.str(k.Name, k.Version, content)

	return t.sum()
}

// maxForceBalance is what every balance read returns under force
// (numscript.MaxForceBalance on the server).
var maxForceBalance = new(big.Int).Lsh(big.NewInt(1), 256)

// NumscriptLatest returns the greatest stored semver of name, or "" when the
// library holds no version of it.
func (s LedgerState) NumscriptLatest(name string) string {
	var (
		best    semver.Version
		bestRaw string
	)

	for k := range s.numscripts.From(NumscriptKey{Name: name}) {
		if k.Name != name {
			break
		}

		v, err := semver.Parse(k.Version)
		if err != nil {
			continue
		}

		if bestRaw == "" || v.Compare(best) > 0 {
			best, bestRaw = v, k.Version
		}
	}

	return bestRaw
}

// NumscriptContent returns the stored content of one exact version.
func (s LedgerState) NumscriptContent(name, version string) (string, bool) {
	return s.numscripts.Get(NumscriptKey{Name: name, Version: version})
}

// Numscripts returns every stored library version.
func (s LedgerState) Numscripts() map[NumscriptKey]string {
	out := make(map[NumscriptKey]string, s.numscripts.Len())
	for k, v := range s.numscripts.All() {
		out[k] = v
	}

	return out
}

// applySaveNumscript mirrors processSaveNumscript's check order: name, content,
// parse, version syntax, then immutability.
func (s *LedgerState) applySaveNumscript(req *servicepb.SaveNumscriptRequest) OrderResult {
	if err := domain.ValidateNumscriptName(req.GetName()); err != nil {
		return OrderResult{Reason: err.Reason()}
	}

	if req.GetContent() == "" {
		return OrderResult{Reason: domain.ErrNumscriptContentRequired.Reason()}
	}

	if parsed := parseScript(req.GetContent()); parsed.parseErr {
		return OrderResult{Reason: domain.ErrReasonNumscriptParseError}
	}

	if _, err := semver.Parse(req.GetVersion()); err != nil {
		return OrderResult{Reason: domain.ErrReasonNumscriptInvalidVersion}
	}

	key := NumscriptKey{Name: req.GetName(), Version: req.GetVersion()}
	if s.numscripts.Has(key) {
		return OrderResult{Reason: domain.ErrReasonNumscriptVersionAlreadyExists}
	}

	s.numscripts = s.numscripts.Set(key, req.GetContent())

	return OrderResult{OK: true, SavedNumscript: &commonpb.NumscriptInfo{
		Name:    req.GetName(),
		Content: req.GetContent(),
		Version: req.GetVersion(),
		Ledger:  req.GetLedger(),
	}}
}

// parsedScript is one script text's parse and compile, which depend on the text
// alone and are shared by every order and every candidate base carrying it.
type parsedScript struct {
	result   numscript.ParseResult
	parseErr bool

	compileOnce sync.Once
	encoder     numscript.VarsEncoder
	compileErr  bool
}

const parsedScriptCacheCap = 4096

var (
	parsedScriptsMu sync.Mutex
	parsedScripts   = map[string]*parsedScript{}
)

func parseScript(text string) *parsedScript {
	parsedScriptsMu.Lock()
	defer parsedScriptsMu.Unlock()

	if p, ok := parsedScripts[text]; ok {
		return p
	}

	if len(parsedScripts) >= parsedScriptCacheCap {
		clear(parsedScripts)
	}

	result := numscript.Parse(text)
	p := &parsedScript{result: result, parseErr: len(result.GetParsingErrors()) > 0}
	parsedScripts[text] = p

	return p
}

// compiles reports whether admission's compile (the VM compiler, then the vars
// encoding against the program's layout) accepts the script with vars.
func (p *parsedScript) compiles(vars map[string]string) bool {
	p.compileOnce.Do(func() {
		encoder, _, err := p.result.Compile()
		p.encoder, p.compileErr = encoder, err != nil
	})

	if p.compileErr {
		return false
	}

	_, err := p.encoder.Encode(vars)

	return err == nil
}

// scriptStore serves the ledger's balances and account metadata to the
// interpreter the way the server's numscript.Store serves its ValueSource:
// balance = input - output per (account, asset, color), every balance
// 2^256 under force, scope-qualified reads refused. readAttempted mirrors
// RecordingStore.MutableReadAttempted.
type scriptStore struct {
	s             *LedgerState
	force         bool
	readAttempted bool
}

func (st *scriptStore) GetBalances(_ context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	if !st.force {
		st.readAttempted = true
	}

	out := make(numscript.Balances, 0, len(q))
	for _, item := range q {
		if item.Scope != "" {
			return nil, domain.ErrScopedBalanceUnsupported
		}

		balance := new(big.Int).Set(maxForceBalance)
		if !st.force {
			vp := st.s.vol(VolumeKey{Address: item.Account, Asset: item.Asset, Color: item.Color})
			balance = new(big.Int).Sub(vp.Input.ToBig(), vp.Output.ToBig())
		}

		out = append(out, numscript.BalanceRow{
			Account: item.Account,
			Asset:   item.Asset,
			Color:   item.Color,
			Scope:   item.Scope,
			Amount:  balance,
		})
	}

	return out, nil
}

func (st *scriptStore) GetAccountsMetadata(_ context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	st.readAttempted = true

	var out numscript.AccountsMetadata
	for _, item := range q {
		if item.Scope != "" {
			return nil, domain.ErrScopedBalanceUnsupported
		}

		for _, key := range item.Keys {
			v, ok := st.s.metadata.Get(MetaKey{Address: item.Account, Key: key})
			if !ok {
				continue
			}

			out = append(out, numscript.AccountMetadataRow{
				Account: item.Account,
				Key:     key,
				Value:   commonpb.MetadataValueToString(v),
			})
		}
	}

	return out, nil
}

// saveOverlay is admission's in-batch view of the saves ahead of an order
// (bulkOverlay.recordNumscriptSave): every save is recorded, valid or not, its
// content shadowing the stored one, and the per-name latest is the first saved
// version until a strictly greater semver follows.
type saveOverlay struct {
	entries map[NumscriptKey]string
	latest  map[string]string
}

func (o *saveOverlay) record(req *servicepb.SaveNumscriptRequest) {
	if o.entries == nil {
		o.entries, o.latest = map[NumscriptKey]string{}, map[string]string{}
	}

	o.entries[NumscriptKey{Name: req.GetName(), Version: req.GetVersion()}] = req.GetContent()

	if cur, ok := o.latest[req.GetName()]; ok && !greaterSemver(req.GetVersion(), cur) {
		return
	}

	o.latest[req.GetName()] = req.GetVersion()
}

// greaterSemver reports whether a is a strictly greater full semver than b; an
// unparsable b loses to any valid a.
func greaterSemver(a, b string) bool {
	av, aerr := semver.Parse(a)
	if aerr != nil {
		return false
	}

	bv, berr := semver.Parse(b)
	if berr != nil {
		return true
	}

	return av.Compare(bv) > 0
}

// scriptOf returns the script a create runs and the reference selector it came
// through ("" for an inline script). ok is false for a postings create. ov is
// admission's view of the batch's earlier saves; nil in the FSM, which reads
// the library as committed so far.
func (s *LedgerState) scriptOf(ct *servicepb.CreateTransactionPayload, ov *saveOverlay) (text string, vars map[string]string, selector string, reason string, ok bool) {
	if ref := ct.GetScriptReference(); ref != nil && ref.GetName() != "" {
		name, version := ref.GetName(), ref.GetVersion()
		if version == "latest" {
			version = s.NumscriptLatest(name)
			if ov != nil {
				if pending, ok := ov.latest[name]; ok && !greaterSemver(version, pending) {
					version = pending
				}
			}

			if version == "" {
				return "", nil, ref.GetVersion(), domain.ErrReasonNumscriptNotFound, true
			}
		}

		content, found := s.NumscriptContent(name, version)
		if ov != nil {
			if pending, ok := ov.entries[NumscriptKey{Name: name, Version: version}]; ok {
				content, found = pending, true
			}
		}

		if !found {
			return "", nil, ref.GetVersion(), domain.ErrReasonNumscriptNotFound, true
		}

		return content, ref.GetVars(), ref.GetVersion(), "", true
	}

	if script := ct.GetScript(); script.GetPlain() != "" {
		return script.GetPlain(), script.GetVars(), "", "", true
	}

	return "", nil, "", "", false
}

// IsScripted reports whether a create runs a script, inline or by reference.
func IsScripted(ct *servicepb.CreateTransactionPayload) bool {
	return ct.GetScriptReference() != nil || ct.GetScript().GetPlain() != ""
}

// scriptAdmission is admission's verdict on one scripted create in state s
// (resolveScriptsAndEnrichNeeds then classifyResolutionFailure): "" lets it
// through, forwarded marks it preload_unavailable, anything else rejects the
// batch. The workload always sends an idempotency key, so the keyed branches of
// the classification are the only ones modelled.
func (s *LedgerState) scriptAdmission(ct *servicepb.CreateTransactionPayload, ov *saveOverlay) (reason string, forwarded bool) {
	text, vars, selector, reason, _ := s.scriptOf(ct, ov)
	if reason != "" {
		return reason, false
	}

	latest := selector == "latest"

	// A freezable failure terminates, except under a "latest" selector, whose
	// keyed retry might replay a version that once succeeded.
	freezable := func(reason string) (string, bool) {
		if latest {
			return "", true
		}

		return reason, false
	}

	parsed := parseScript(text)
	if parsed.parseErr {
		return freezable(domain.ErrReasonNumscriptParseError)
	}

	store := &scriptStore{s: s, force: ct.GetForce()}
	if _, err := parsed.result.ResolveDependencies(context.Background(), vars, store); err != nil {
		var described domain.SerializableError
		switch {
		case errors.Is(err, numscript.ErrScalingNotSupported):
			return freezable(domain.ErrNumscriptScalingUnsupported.Reason())
		case errors.As(err, &described) && domain.IsFreezableFailure(described.Kind()):
			return freezable(described.Reason())
		case store.readAttempted || latest:
			// A script-caused failure is a precondition, but one raised after a
			// mutable read is forwarded under a key like any state-dependent one.
			return "", true
		case isScriptError(err):
			return domain.ErrReasonNumscriptExecutionError, false
		default:
			return domain.ErrReasonNumscriptRuntime, false
		}
	}

	if !parsed.compiles(vars) {
		return freezable(domain.ErrReasonNumscriptCompileError)
	}

	return "", false
}

// scriptProduct is what a committed script hands processCreateTransaction.
type scriptProduct struct {
	postings    []*commonpb.Posting
	txMeta      map[string]*commonpb.MetadataValue
	accountMeta map[string]map[string]*commonpb.MetadataValue
}

// produceScript runs a scripted create against s and folds its postings into the
// volumes, mirroring numscriptPostingProducer.produce: the run, then per posting
// the scope, sign and width checks and the overflow-checked volume update, then
// the script's metadata validation.
func (s *LedgerState) produceScript(ct *servicepb.CreateTransactionPayload, touched map[VolumeKey]bool) (scriptProduct, map[VolumeKey]VolumePair, string) {
	text, vars, _, reason, _ := s.scriptOf(ct, nil)
	if reason != "" {
		return scriptProduct{}, nil, reason
	}

	store := &scriptStore{s: s, force: ct.GetForce()}

	result, runErr := parseScript(text).result.Run(context.Background(), vars, store)
	if runErr != nil {
		var missing numscript.MissingFundsErr
		if errors.As(runErr, &missing) {
			return scriptProduct{}, nil, domain.ErrReasonInsufficientFunds
		}

		if isScriptError(runErr) {
			return scriptProduct{}, nil, domain.ErrReasonNumscriptExecutionError
		}

		var described domain.SerializableError
		if errors.As(runErr, &described) {
			return scriptProduct{}, nil, described.Reason()
		}

		return scriptProduct{}, nil, domain.ErrReasonNumscriptRuntime
	}

	pcv := map[VolumeKey]VolumePair{}
	postings := make([]*commonpb.Posting, len(result.Postings))

	for i, p := range result.Postings {
		if p.SourceScope != "" || p.DestinationScope != "" {
			return scriptProduct{}, nil, domain.ErrScopedBalanceUnsupported.Reason()
		}

		if p.Amount.Sign() < 0 {
			return scriptProduct{}, nil, domain.ErrReasonNumscriptExecutionError
		}

		var amt uint256.Int
		if amt.SetFromBig(p.Amount) {
			return scriptProduct{}, nil, domain.ErrReasonNumscriptExecutionError
		}

		posting := &commonpb.Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      commonpb.NewUint256(&amt),
			Asset:       p.Asset,
			Color:       p.Color,
		}
		postings[i] = posting

		// The interpreter decided funding; the FSM only guards the counters.
		if _, reason := s.applyPostingsInto(pcv, []*commonpb.Posting{posting}, true, touched); reason != "" {
			return scriptProduct{}, nil, reason
		}
	}

	var accountMeta map[string]map[string]*commonpb.MetadataValue
	for _, row := range result.AccountsMetadata {
		if row.Scope != "" {
			return scriptProduct{}, nil, domain.ErrScopedBalanceUnsupported.Reason()
		}

		if err := domain.ValidateMetadataKey(row.Key); err != nil {
			return scriptProduct{}, nil, err.Reason()
		}

		if err := domain.ValidateMetadataString(row.Value); err != nil {
			return scriptProduct{}, nil, err.Reason()
		}

		if accountMeta == nil {
			accountMeta = map[string]map[string]*commonpb.MetadataValue{}
		}

		if accountMeta[row.Account] == nil {
			accountMeta[row.Account] = map[string]*commonpb.MetadataValue{}
		}

		accountMeta[row.Account][row.Key] = commonpb.NewStringValue(row.Value)
	}

	var txMeta map[string]*commonpb.MetadataValue
	for _, key := range slices.Sorted(maps.Keys(result.Metadata)) {
		if err := domain.ValidateMetadataKey(key); err != nil {
			return scriptProduct{}, nil, err.Reason()
		}

		if err := domain.ValidateMetadataString(result.Metadata[key]); err != nil {
			return scriptProduct{}, nil, err.Reason()
		}

		if txMeta == nil {
			txMeta = map[string]*commonpb.MetadataValue{}
		}

		txMeta[key] = commonpb.NewStringValue(result.Metadata[key])
	}

	return scriptProduct{postings: postings, txMeta: txMeta, accountMeta: accountMeta}, pcv, ""
}

// validatePostingFields mirrors processCreateTransaction's validatePostings,
// which covers script-resolved addresses and assets.
func validatePostingFields(postings []*commonpb.Posting) string {
	for _, p := range postings {
		for _, err := range []domain.SerializableError{
			domain.ValidateAccountAddress(p.GetSource()),
			domain.ValidateAccountAddress(p.GetDestination()),
			domain.ValidateAsset(p.GetAsset()),
			domain.ValidateColor(p.GetColor()),
		} {
			if err != nil {
				return err.Reason()
			}
		}
	}

	return ""
}

// mergeScriptMetadata layers the caller's metadata over the script's: the caller
// wins every collision, for the transaction and per account alike.
func mergeScriptMetadata(ct *servicepb.CreateTransactionPayload, product scriptProduct) (map[string]*commonpb.MetadataValue, map[string]*commonpb.MetadataMap) {
	txMeta := ct.GetMetadata()
	if len(product.txMeta) > 0 {
		merged := maps.Clone(product.txMeta)
		maps.Copy(merged, ct.GetMetadata())
		txMeta = merged
	}

	var accountMeta map[string]*commonpb.MetadataMap
	for account, values := range product.accountMeta {
		if accountMeta == nil {
			accountMeta = map[string]*commonpb.MetadataMap{}
		}

		accountMeta[account] = &commonpb.MetadataMap{Values: maps.Clone(values)}
	}

	for account, mm := range ct.GetAccountMetadata() {
		if accountMeta == nil {
			accountMeta = map[string]*commonpb.MetadataMap{}
		}

		existing := accountMeta[account]
		if existing == nil {
			accountMeta[account] = &commonpb.MetadataMap{Values: maps.Clone(mm.GetValues())}

			continue
		}

		maps.Copy(existing.Values, mm.GetValues())
	}

	return txMeta, accountMeta
}

// createContentReason mirrors admission's validateOrderContent: a create needs
// exactly one content source, and an executable reference names "latest" or a
// full semver.
func createContentReason(ct *servicepb.CreateTransactionPayload) string {
	if ct == nil {
		return ""
	}

	hasPostings := len(ct.GetPostings()) > 0
	hasInline := ct.GetScript().GetPlain() != ""
	refPresent := ct.GetScriptReference() != nil
	refValid := refPresent && ct.GetScriptReference().GetName() != ""

	switch {
	case hasPostings && (hasInline || refPresent):
		return domain.ErrPostingsAndScriptConflict.Reason()
	case !hasPostings && !hasInline && !refValid:
		return domain.ErrEmptyTransaction.Reason()
	}

	if refValid {
		if v := ct.GetScriptReference().GetVersion(); v != "latest" {
			if _, err := semver.Parse(v); err != nil {
				return domain.ErrReasonNumscriptInvalidVersion
			}
		}
	}

	return ""
}

// admitScripts runs admission's script pass over a bulk that passed the
// structural gates: each scripted create, in order, is resolved against the
// state the orders before it leave behind (admission's batch overlay), and
// either rejects the batch or is forwarded to fail in the FSM with
// PRELOAD_UNAVAILABLE. An order admission predicts the FSM will skip is not
// resolved at all.
func (g GlobalState) admitScripts(bulk Bulk) (forwarded map[int]bool, reason string) {
	if !slices.ContainsFunc(bulk.Requests, func(req *servicepb.Request) bool {
		return IsScripted(req.GetApply().GetAction().GetCreateTransaction())
	}) {
		return nil, ""
	}

	scratch := g.clone()
	overlays := map[string]*saveOverlay{}
	for i, req := range bulk.Requests {
		name := LedgerOf(req)
		ls, ok := scratch.ledgers[name]
		if !ok {
			ls = NewLedgerState()
		}

		if ct := req.GetApply().GetAction().GetCreateTransaction(); IsScripted(ct) {
			if ref := ct.GetReference(); ref != "" && ls.txByRef.Has(ref) &&
				slices.Contains(req.GetApply().GetSkippableReasons(), domain.ReasonCode(domain.ErrReasonTransactionReferenceConflict)) {
				continue
			}

			reason, fwd := ls.scriptAdmission(ct, overlays[name])
			if reason != "" {
				return nil, reason
			}

			if fwd {
				if forwarded == nil {
					forwarded = map[int]bool{}
				}

				forwarded[i] = true

				continue
			}
		}

		if save := req.GetSaveNumscript(); save != nil {
			if overlays[name] == nil {
				overlays[name] = &saveOverlay{}
			}

			overlays[name].record(save)
		}

		if !ledgerScopedRequest(req) {
			continue
		}

		before := ls
		if oc := ls.applyOne(req, map[VolumeKey]bool{}, uint64(ls.txs.Len())); !oc.OK {
			ls = before
		}

		scratch.ledgers[name] = ls
	}

	return forwarded, ""
}

// ledgerScopedRequest reports whether applyOne handles req: everything but the
// lifecycle and checkpoint orders GlobalState applies itself.
func ledgerScopedRequest(req *servicepb.Request) bool {
	switch req.GetType().(type) {
	case *servicepb.Request_SetMaintenanceMode, *servicepb.Request_CreateLedger,
		*servicepb.Request_DeleteLedger, *servicepb.Request_PromoteLedger,
		*servicepb.Request_CreateQueryCheckpoint, *servicepb.Request_DeleteQueryCheckpoint,
		*servicepb.Request_SetQueryCheckpointSchedule, *servicepb.Request_DeleteQueryCheckpointSchedule:
		return false
	}

	return true
}

// isScriptError reports whether an interpreter error is a failure the script
// caused — its text, its vars, or the balances and metadata it read — which the
// server reports as NUMSCRIPT_EXECUTION_ERROR (convertNumscriptError,
// convertVMError). Library defects stay NUMSCRIPT_RUNTIME.
func isScriptError(err error) bool {
	return isErrorType[numscript.NegativeAmountErr](err) ||
		isErrorType[numscript.MissingVariableErr](err) ||
		isErrorType[numscript.InvalidAccountName](err) ||
		isErrorType[numscript.InvalidAsset](err) ||
		isErrorType[numscript.InvalidColor](err) ||
		isErrorType[numscript.InvalidScope](err) ||
		isErrorType[numscript.InvalidMonetaryLiteral](err) ||
		isErrorType[numscript.InvalidNumberLiteral](err) ||
		isErrorType[numscript.BadPortionParsingErr](err) ||
		isErrorType[numscript.MismatchedCurrencyError](err) ||
		isErrorType[numscript.DivideByZero](err) ||
		isErrorType[numscript.TypeError](err) ||
		isErrorType[numscript.MetadataNotFound](err) ||
		isErrorType[numscript.NegativeBalanceError](err) ||
		isErrorType[numscript.InvalidAllotmentSum](err) ||
		isErrorType[numscript.NegativePortion](err) ||
		isErrorType[numscript.InvalidRemainingAllotment](err) ||
		isErrorType[numscript.InvalidAllotmentInSendAll](err) ||
		isErrorType[numscript.InvalidUnboundedInSendAll](err) ||
		isErrorType[numscript.InvalidUnboundedAddressInScalingAddress](err) ||
		isErrorType[numscript.InvalidNestedMeta](err) ||
		isErrorType[numscript.CannotCastToString](err) ||
		isErrorType[numscript.CannotCastScopedAccountToString](err) ||
		isErrorType[numscript.CannotStoreScopedAccountInMeta](err) ||
		isErrorType[numscript.UnboundVariableErr](err) ||
		isErrorType[numscript.UnboundFunctionErr](err) ||
		isErrorType[numscript.BadArityErr](err) ||
		isErrorType[numscript.InvalidTypeErr](err) ||
		isErrorType[numscript.ExperimentalFeature](err) ||
		isErrorType[numscript.InvalidFeature](err)
}

func isErrorType[T error](err error) bool {
	var target T

	return errors.As(err, &target)
}
