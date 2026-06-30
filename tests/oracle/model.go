package oracle

import (
	"fmt"
	"io"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TypeState is the model's view of one account type.
type TypeState struct {
	Name        string
	Pattern     string
	Persistence commonpb.AccountTypePersistence
}

// VolumeKey is one (address, asset) cell of the volume table.
type VolumeKey struct {
	Address string
	Asset   string
}

// MetaKey is one (address, key) cell of the account-metadata table.
type MetaKey struct {
	Address string
	Key     string
}

// CompareMetaKey compares MetaKeys by address, then key.
func CompareMetaKey(a, b MetaKey) int {
	if c := strings.Compare(a.Address, b.Address); c != 0 {
		return c
	}

	return strings.Compare(a.Key, b.Key)
}

// TxMetaKey is one (transaction reference, key) cell of the transaction-metadata
// table. Transactions are addressed by reference (carried in the request) rather
// than by server-assigned id, so the model can track them in the serialization
// search where no response — hence no id — is available.
type TxMetaKey struct {
	Reference string
	Key       string
}

// CompareTxMetaKey compares TxMetaKeys by reference, then key.
func CompareTxMetaKey(a, b TxMetaKey) int {
	if c := strings.Compare(a.Reference, b.Reference); c != 0 {
		return c
	}

	return strings.Compare(a.Key, b.Key)
}

// VolumePair is the cumulative input/output for one (address, asset) cell.
type VolumePair struct {
	Input  uint256.Int
	Output uint256.Int
}

// CompareVolumeKey compares VolumeKeys by address, then asset.
func CompareVolumeKey(a, b VolumeKey) int {
	if c := strings.Compare(a.Address, b.Address); c != 0 {
		return c
	}

	return strings.Compare(a.Asset, b.Asset)
}

// LedgerState is one ledger's slice of the model: its chart of account types and
// per-cell volumes. Every mutation returns a NEW LedgerState (copy-on-write) so
// the checker can fork it across hypothesized serializations without disturbing
// shared state. Volume values are uint256.Int value types, so a shallow map copy
// fully copies them — forks never alias.
type LedgerState struct {
	types      map[string]TypeState
	volumes    map[VolumeKey]VolumePair
	metadata   map[MetaKey]*commonpb.MetadataValue
	ledgerMeta map[string]*commonpb.MetadataValue
	// Declared metadata field types per key, driving value coercion. Keyed by
	// metadata key (the schema is per (target, key), not per address).
	accountFieldTypes map[string]commonpb.MetadataType
	ledgerFieldTypes  map[string]commonpb.MetadataType

	// Transactions addressed by reference: txRefs maps a reference to its record
	// (id, postings, reverted status), txMeta holds their metadata. txIDToRef maps
	// each reference's server id back to the reference — the API now targets
	// transactions by id (the proto dropped reference targeting in #462), so the
	// generator resolves a tracked reference to its id and the apply path resolves
	// the id back to the reference to reuse the reference-keyed bookkeeping.
	txRefs                map[string]*txRecord
	txMeta                map[TxMetaKey]*commonpb.MetadataValue
	txIDToRef             map[uint64]string
	transactionFieldTypes map[string]commonpb.MetadataType

	// Next transaction id the server assigns in this ledger (starts at 1, per
	// processor_ledger.go), bumped per committed CreateTransaction so the model
	// can predict the id echoed in the response.
	nextTxID uint64
}

func NewLedgerState() LedgerState {
	return LedgerState{
		types:             map[string]TypeState{},
		volumes:           map[VolumeKey]VolumePair{},
		metadata:          map[MetaKey]*commonpb.MetadataValue{},
		ledgerMeta:        map[string]*commonpb.MetadataValue{},
		accountFieldTypes: map[string]commonpb.MetadataType{},
		ledgerFieldTypes:  map[string]commonpb.MetadataType{},

		txRefs:                map[string]*txRecord{},
		txMeta:                map[TxMetaKey]*commonpb.MetadataValue{},
		txIDToRef:             map[uint64]string{},
		transactionFieldTypes: map[string]commonpb.MetadataType{},
		nextTxID:              1,
	}
}

// clone returns a copy whose maps can be mutated independently. TypeState and
// VolumePair are value types, so copying the map copies them. Metadata values are
// *MetadataValue pointers shared across forks — safe because a stored value is
// never mutated in place (a set replaces the entry, a delete removes it).
func (s LedgerState) clone() LedgerState {
	types := make(map[string]TypeState, len(s.types))
	maps.Copy(types, s.types)

	volumes := make(map[VolumeKey]VolumePair, len(s.volumes))
	maps.Copy(volumes, s.volumes)

	metadata := make(map[MetaKey]*commonpb.MetadataValue, len(s.metadata))
	maps.Copy(metadata, s.metadata)

	ledgerMeta := make(map[string]*commonpb.MetadataValue, len(s.ledgerMeta))
	maps.Copy(ledgerMeta, s.ledgerMeta)

	accountFieldTypes := make(map[string]commonpb.MetadataType, len(s.accountFieldTypes))
	maps.Copy(accountFieldTypes, s.accountFieldTypes)

	ledgerFieldTypes := make(map[string]commonpb.MetadataType, len(s.ledgerFieldTypes))
	maps.Copy(ledgerFieldTypes, s.ledgerFieldTypes)

	// Records are replaced (not mutated in place) on a revert, so clones can
	// share the pointer.
	txRefs := make(map[string]*txRecord, len(s.txRefs))
	maps.Copy(txRefs, s.txRefs)

	txMeta := make(map[TxMetaKey]*commonpb.MetadataValue, len(s.txMeta))
	maps.Copy(txMeta, s.txMeta)

	txIDToRef := make(map[uint64]string, len(s.txIDToRef))
	maps.Copy(txIDToRef, s.txIDToRef)

	transactionFieldTypes := make(map[string]commonpb.MetadataType, len(s.transactionFieldTypes))
	maps.Copy(transactionFieldTypes, s.transactionFieldTypes)

	return LedgerState{
		types:                 types,
		volumes:               volumes,
		metadata:              metadata,
		ledgerMeta:            ledgerMeta,
		accountFieldTypes:     accountFieldTypes,
		ledgerFieldTypes:      ledgerFieldTypes,
		txRefs:                txRefs,
		txMeta:                txMeta,
		txIDToRef:             txIDToRef,
		transactionFieldTypes: transactionFieldTypes,
		nextTxID:              s.nextTxID,
	}
}

// compiled compiles the current chart into the server's matcher form. Recomputed
// on demand because a chart op earlier in the same bulk can change it.
func (s *LedgerState) compiled() []accounttype.CompiledType {
	pb := make(map[string]*commonpb.AccountType, len(s.types))
	for name, t := range s.types {
		pb[name] = &commonpb.AccountType{Name: t.Name, Pattern: t.Pattern}
	}

	return accounttype.CompileTypes(pb)
}

// match resolves addr to the type the server would pick (highest specificity,
// etc. — delegated to accounttype), or nil. compiled is passed in so a caller
// validating several addresses compiles the chart once.
func (s *LedgerState) match(addr string, compiled []accounttype.CompiledType) *TypeState {
	best := accounttype.FindMatchingType(addr, compiled)
	if best == nil {
		return nil
	}

	t := s.types[best.GetName()]

	return &t
}

// hash writes a canonical identity of the ledger's state into h.
func (s LedgerState) Hash(h io.Writer) {
	names := make([]string, 0, len(s.types))
	for n := range s.types {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t := s.types[n]
		_, _ = fmt.Fprintf(h, "T|%s|%s|%d\n", t.Name, t.Pattern, t.Persistence)
	}

	keys := make([]VolumeKey, 0, len(s.volumes))
	for k := range s.volumes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return CompareVolumeKey(keys[i], keys[j]) < 0 })
	for _, k := range keys {
		v := s.volumes[k]
		_, _ = fmt.Fprintf(h, "V|%s|%s|%s|%s\n", k.Address, k.Asset, v.Input.Dec(), v.Output.Dec())
	}

	mkeys := make([]MetaKey, 0, len(s.metadata))
	for k := range s.metadata {
		mkeys = append(mkeys, k)
	}
	sort.Slice(mkeys, func(i, j int) bool { return CompareMetaKey(mkeys[i], mkeys[j]) < 0 })
	for _, k := range mkeys {
		_, _ = fmt.Fprintf(h, "M|%s|%s|%s\n", k.Address, k.Key, MetaValueString(s.metadata[k]))
	}

	lkeys := make([]string, 0, len(s.ledgerMeta))
	for k := range s.ledgerMeta {
		lkeys = append(lkeys, k)
	}
	sort.Strings(lkeys)
	for _, k := range lkeys {
		_, _ = fmt.Fprintf(h, "LM|%s|%s\n", k, MetaValueString(s.ledgerMeta[k]))
	}

	hashFieldTypes(h, "AF", s.accountFieldTypes)
	hashFieldTypes(h, "LF", s.ledgerFieldTypes)
	hashFieldTypes(h, "TF", s.transactionFieldTypes)

	refs := make([]string, 0, len(s.txRefs))
	for r := range s.txRefs {
		refs = append(refs, r)
	}
	sort.Strings(refs)
	for _, r := range refs {
		rev := ""
		if s.txRefs[r].reverted {
			rev = "R"
		}
		_, _ = fmt.Fprintf(h, "TR|%s|%s\n", r, rev)
	}

	tkeys := make([]TxMetaKey, 0, len(s.txMeta))
	for k := range s.txMeta {
		tkeys = append(tkeys, k)
	}
	sort.Slice(tkeys, func(i, j int) bool { return CompareTxMetaKey(tkeys[i], tkeys[j]) < 0 })
	for _, k := range tkeys {
		_, _ = fmt.Fprintf(h, "TM|%s|%s|%s\n", k.Reference, k.Key, MetaValueString(s.txMeta[k]))
	}
}

// hashFieldTypes writes a tag-prefixed, key-sorted rendering of a field-type map.
func hashFieldTypes(h io.Writer, tag string, types map[string]commonpb.MetadataType) {
	keys := make([]string, 0, len(types))
	for k := range types {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(h, "%s|%s|%d\n", tag, k, types[k])
	}
}

// MetaValueString renders a metadata value as a canonical, type-tagged string,
// used for both hashing and equality: two values are equal iff their renderings
// match. The type tag keeps a string "5" distinct from an int 5.
func MetaValueString(v *commonpb.MetadataValue) string {
	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return "s:" + t.StringValue
	case *commonpb.MetadataValue_IntValue:
		return "i:" + strconv.FormatInt(t.IntValue, 10)
	case *commonpb.MetadataValue_UintValue:
		return "u:" + strconv.FormatUint(t.UintValue, 10)
	case *commonpb.MetadataValue_BoolValue:
		return "b:" + strconv.FormatBool(t.BoolValue)
	case *commonpb.MetadataValue_NullValue:
		return "n:" + t.NullValue.GetOriginal()
	default:
		return "<nil>"
	}
}

// matchAddress resolves addr to its account type in this state (compiling the
// chart fresh), or nil. Convenience for callers that match a single address.
func (s *LedgerState) MatchAddress(addr string) *TypeState {
	return s.match(addr, s.compiled())
}

// vol returns the cell's volumes, or the zero pair (zero uint256s) if absent.
func (s *LedgerState) vol(key VolumeKey) VolumePair {
	return s.volumes[key]
}

// accountMetadata returns addr's metadata as a key→value map (empty if none).
func (s *LedgerState) AccountMetadata(addr string) map[string]*commonpb.MetadataValue {
	out := map[string]*commonpb.MetadataValue{}
	for mk, v := range s.metadata {
		if mk.Address == addr {
			out[mk.Key] = v
		}
	}

	return out
}

// GlobalState is the committed state across all ledgers — one LedgerState per
// ledger. It mirrors the single Raft log: bulks commit to the cluster in one
// global order, and each request in a bulk mutates its own ledger's sub-state.
//
// This is the pure forward model: given a state and a bulk, Apply predicts
// exactly what the server would do (per-request success/failure + the resulting
// state, atomically across whatever ledgers the bulk touches). It is deliberately
// separate from the checker's bookkeeping (in-flight set, re-order buffer,
// observations) so it can be unit-tested and forked.
type GlobalState struct {
	ledgers map[string]LedgerState
}

func NewGlobalState() GlobalState {
	return GlobalState{ledgers: map[string]LedgerState{}}
}

// clone deep-copies each ledger so forks never share mutable state.
func (g GlobalState) clone() GlobalState {
	m := make(map[string]LedgerState, len(g.ledgers))
	for name, ls := range g.ledgers {
		m[name] = ls.clone()
	}

	return GlobalState{ledgers: m}
}

// ledger returns the named ledger's state, or an empty one if untouched.
func (g GlobalState) Ledger(name string) LedgerState {
	if ls, ok := g.ledgers[name]; ok {
		return ls
	}

	return NewLedgerState()
}

// hash writes a canonical identity across all non-empty ledgers into h.
// candidateBases dedups on the resulting 64-bit hash, collapsing bases reached
// via different (commutative) serializations.
func (g GlobalState) Hash(h io.Writer) {
	names := make([]string, 0, len(g.ledgers))
	for n := range g.ledgers {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		ls := g.ledgers[n]
		if len(ls.types) == 0 && len(ls.volumes) == 0 && len(ls.metadata) == 0 && len(ls.ledgerMeta) == 0 &&
			len(ls.accountFieldTypes) == 0 && len(ls.ledgerFieldTypes) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(h, "L|%s\n", n)
		ls.Hash(h)
	}
}

// OrderResult is the predicted outcome of one request in a bulk. PCV holds the
// touched cells' post-commit volumes for a committed transaction (the server
// returns these per-tx); it is nil for non-transaction orders. Meta holds the
// predicted metadata effect for a committed metadata write, checked against the
// server's response log; it is nil for non-metadata orders.
type OrderResult struct {
	OK     bool
	Reason string
	PCV    map[VolumeKey]VolumePair
	Meta   *metaEffect
	// TxID is the id the server assigns to a committed CreateTransaction or the
	// new revert transaction (0 for any other order), checked against the log.
	TxID uint64
	// Revert is set for a committed RevertTransaction: the original id and the
	// predicted reversed postings, checked against the RevertedTransaction log.
	Revert *revertEffect
}

// metaEffect is a metadata write's predicted effect, for asserting the server's
// response log: the as-written values it should have stored (saved). Stored
// values are verbatim — the declared type is applied only on read.
type metaEffect struct {
	saved map[string]*commonpb.MetadataValue
}

// txRecord is a committed transaction tracked by reference: its server-assigned
// id and postings (to predict a revert's reversed postings) and whether it has
// been reverted (a second revert is rejected). Records are replaced, never
// mutated in place, so clones safely share the pointer.
type txRecord struct {
	id       uint64
	postings []*commonpb.Posting
	reverted bool
}

// revertEffect is a committed revert's predicted effect: the original
// transaction id (echoed as reverted_transaction_id) and the reversed postings.
type revertEffect struct {
	revertedID uint64
	postings   []*commonpb.Posting
}

// ApplyResult is the predicted outcome of applying a whole bulk.
//   - OK: the bulk committed.
//   - Reason: the rejection reason (domain.ErrReason*) when !OK — either the
//     first failing order's reason or an end-of-bulk reason
//     (TRANSIENT_ACCOUNT_NON_ZERO) not attributable to a single order.
//   - State: the resulting state (equals the input state when !OK).
//   - Orders: per-request detail, index-aligned with bulk.Requests, truncated
//     at the first failing order.
type ApplyResult struct {
	OK     bool
	Reason string
	State  GlobalState
	Orders []OrderResult
}

// LedgerOf returns the ledger a request targets.
func LedgerOf(req *servicepb.Request) string {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		return r.Apply.GetLedger()
	case *servicepb.Request_AddAccountType:
		return r.AddAccountType.GetLedger()
	case *servicepb.Request_RemoveAccountType:
		return r.RemoveAccountType.GetLedger()
	case *servicepb.Request_SaveLedgerMetadata:
		return r.SaveLedgerMetadata.GetLedger()
	case *servicepb.Request_DeleteLedgerMetadata:
		return r.DeleteLedgerMetadata.GetLedger()
	case *servicepb.Request_SetMetadataFieldType:
		return r.SetMetadataFieldType.GetLedger()
	case *servicepb.Request_RemoveMetadataFieldType:
		return r.RemoveMetadataFieldType.GetLedger()
	default:
		panic(fmt.Sprintf("LedgerOf: unmodeled request type %T", req.GetType()))
	}
}

// Apply folds bulk's requests into g in order, predicting each one. The server
// applies a bulk atomically (one Raft entry): the first failing request — or an
// end-of-bulk transient violation on any touched ledger — rejects the whole bulk
// and leaves every ledger unchanged. A bulk may span ledgers; each request is
// routed to its own ledger's sub-state and the end-of-bulk checks run per ledger.
func (g GlobalState) Apply(bulk Bulk) ApplyResult {
	next := g.clone()
	orders := make([]OrderResult, 0, len(bulk.Requests))
	touched := map[string]map[VolumeKey]bool{}

	for _, req := range bulk.Requests {
		name := LedgerOf(req)

		ls, ok := next.ledgers[name]
		if !ok {
			ls = NewLedgerState()
			next.ledgers[name] = ls
		}

		cells := touched[name]
		if cells == nil {
			cells = map[VolumeKey]bool{}
			touched[name] = cells
		}

		oc := ls.applyOne(req, cells)
		// ls is a value copy out of the map; its maps are shared with
		// next.ledgers[name] (so map mutations already took effect), but value
		// fields it mutates (nextTxID) must be written back explicitly.
		next.ledgers[name] = ls
		orders = append(orders, oc)

		if !oc.OK {
			// Atomic bulk: discard the working copy, nothing commits.
			return ApplyResult{OK: false, Reason: oc.Reason, State: g, Orders: orders}
		}
	}

	// End-of-bulk write-set semantics, per touched ledger: a TRANSIENT cell left
	// non-zero rejects the whole bulk; otherwise zero-balance EPHEMERAL/TRANSIENT
	// cells are purged.
	for name, cells := range touched {
		ls := next.ledgers[name]
		base := g.Ledger(name)

		if reason, violated := ls.transientViolation(&base, cells); violated {
			return ApplyResult{OK: false, Reason: reason, State: g, Orders: orders}
		}

		ls.purgeZeroBalance(cells)
	}

	return ApplyResult{OK: true, State: next, Orders: orders}
}

// applyOne mutates the (already-forked) working state for one request and
// returns its predicted outcome, recording touched volume cells.
func (s *LedgerState) applyOne(req *servicepb.Request, touched map[VolumeKey]bool) OrderResult {
	switch r := req.GetType().(type) {
	case *servicepb.Request_AddAccountType:
		at := r.AddAccountType.GetAccountType()
		name := at.GetName()
		if _, exists := s.types[name]; exists {
			return OrderResult{Reason: domain.ErrReasonAccountTypeAlreadyExists}
		}

		s.types[name] = TypeState{Name: name, Pattern: at.GetPattern(), Persistence: at.GetPersistence()}

		return OrderResult{OK: true}

	case *servicepb.Request_RemoveAccountType:
		name := r.RemoveAccountType.GetName()
		if _, exists := s.types[name]; !exists {
			return OrderResult{Reason: domain.ErrReasonAccountTypeNotFound}
		}

		delete(s.types, name)

		return OrderResult{OK: true}

	case *servicepb.Request_SaveLedgerMetadata:
		return s.applySaveLedgerMetadata(r.SaveLedgerMetadata)

	case *servicepb.Request_DeleteLedgerMetadata:
		return s.applyDeleteLedgerMetadata(r.DeleteLedgerMetadata)

	case *servicepb.Request_SetMetadataFieldType:
		return s.applySetMetadataFieldType(r.SetMetadataFieldType)

	case *servicepb.Request_RemoveMetadataFieldType:
		return s.applyRemoveMetadataFieldType(r.RemoveMetadataFieldType)

	case *servicepb.Request_Apply:
		switch a := r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return s.applyTransaction(a.CreateTransaction, touched)
		case *servicepb.LedgerAction_AddMetadata:
			return s.applyAddMetadata(a.AddMetadata)
		case *servicepb.LedgerAction_DeleteMetadata:
			return s.applyDeleteMetadata(a.DeleteMetadata)
		case *servicepb.LedgerAction_RevertTransaction:
			return s.applyRevert(a.RevertTransaction, touched)
		default:
			// The generator emits only the actions above; any other is unmodeled
			// — fail loudly, the generator and model must stay in lockstep.
			// TODO(model): SetDefaultEnforcementMode.
			panic(fmt.Sprintf("model: unmodeled LedgerApply action %T", r.Apply.GetAction().GetData()))
		}

	default:
		// The generator emits only Add/RemoveAccountType and Apply; any other
		// top-level request is unmodeled.
		// TODO(model): top-level chart/enforcement-mode requests.
		panic(fmt.Sprintf("model: unmodeled request type %T", req.GetType()))
	}
}

// applyTransaction predicts a CreateTransaction: STRICT chart enforcement
// (every non-world address must match a type once any type exists), then volume
// accumulation. Insufficient-funds is not modelled because the workload only
// debits "world" (overdraftable) or uses Force.
func (s *LedgerState) applyTransaction(ct *servicepb.CreateTransactionPayload, touched map[VolumeKey]bool) OrderResult {
	if s.chartRejects(ct.GetPostings()) {
		return OrderResult{Reason: domain.ErrReasonAccountNotMatchingType}
	}

	pcv := s.applyPostings(ct.GetPostings(), touched)

	// A reference becomes targetable. A duplicate rejects the whole bulk; the
	// workload uses unique references, so this guard is never exercised.
	ref := ct.GetReference()
	if ref != "" {
		if _, exists := s.txRefs[ref]; exists {
			return OrderResult{Reason: domain.ErrReasonTransactionReferenceConflict}
		}
	}

	id := s.nextTxID
	s.nextTxID++

	var meta *metaEffect

	if ref != "" {
		s.txRefs[ref] = &txRecord{id: id, postings: ct.GetPostings()}
		s.txIDToRef[id] = ref

		// Metadata is stored verbatim; the declared type is applied only on read.
		if len(ct.GetMetadata()) > 0 {
			saved := make(map[string]*commonpb.MetadataValue, len(ct.GetMetadata()))
			for key, val := range ct.GetMetadata() {
				s.txMeta[TxMetaKey{Reference: ref, Key: key}] = val
				saved[key] = val
			}

			meta = &metaEffect{saved: saved}
		}
	}

	return OrderResult{OK: true, PCV: pcv, Meta: meta, TxID: id}
}

// applyRevert predicts a RevertTransaction: it reverses the original postings
// (swap source/destination), enforces the chart on them (force skips only the
// balance check, which the model does not track), moves the volumes, marks the
// original reverted, and consumes a new transaction id for the revert itself.
func (s *LedgerState) applyRevert(rt *servicepb.RevertTransactionPayload, touched map[VolumeKey]bool) OrderResult {
	ref, ok := s.txIDToRef[rt.GetTransactionId()]
	if !ok {
		// Unknown id (id >= NextTransactionId); the server rejects with
		// TRANSACTION_NOT_FOUND. The generator targets committed transactions, so
		// in commit order this is unreachable, but a candidate-base ordering may
		// not have applied the create yet.
		return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
	}

	rec := s.txRefs[ref]

	if rec.reverted {
		return OrderResult{Reason: domain.ErrReasonTransactionAlreadyReverted}
	}

	reversed := make([]*commonpb.Posting, len(rec.postings))
	for i, p := range rec.postings {
		reversed[i] = &commonpb.Posting{
			Source:      p.GetDestination(),
			Destination: p.GetSource(),
			Amount:      p.GetAmount(),
			Asset:       p.GetAsset(),
		}
	}

	if s.chartRejects(reversed) {
		return OrderResult{Reason: domain.ErrReasonAccountNotMatchingType}
	}

	pcv := s.applyPostings(reversed, touched)

	id := s.nextTxID
	s.nextTxID++
	// Replace (don't mutate) so clones sharing the pointer are unaffected.
	s.txRefs[ref] = &txRecord{id: rec.id, postings: rec.postings, reverted: true}

	return OrderResult{
		OK:     true,
		PCV:    pcv,
		TxID:   id,
		Revert: &revertEffect{revertedID: rec.id, postings: reversed},
	}
}

// chartRejects reports whether any non-world address in postings fails to match
// the chart. Enforcement only applies once the chart is non-empty (the server's
// validateAccountAgainstAccountTypes short-circuits on an empty chart); the
// default mode is STRICT, which the workload never changes.
func (s *LedgerState) chartRejects(postings []*commonpb.Posting) bool {
	compiled := s.compiled()
	if len(compiled) == 0 {
		return false
	}

	for _, p := range postings {
		for _, addr := range []string{p.GetSource(), p.GetDestination()} {
			if addr == "world" {
				continue
			}
			if s.match(addr, compiled) == nil {
				return true
			}
		}
	}

	return false
}

// applyPostings accumulates postings into volumes (source.output += amount,
// destination.input += amount) read-modify-write per cell so postings touching
// the same cell compose, returning the post-commit volumes of the touched cells.
func (s *LedgerState) applyPostings(postings []*commonpb.Posting, touched map[VolumeKey]bool) map[VolumeKey]VolumePair {
	pcv := map[VolumeKey]VolumePair{}
	bump := func(key VolumeKey, addIn, addOut *uint256.Int) {
		cur := s.vol(key)
		cur.Input.Add(&cur.Input, addIn)
		cur.Output.Add(&cur.Output, addOut)
		s.volumes[key] = cur
		touched[key] = true
		pcv[key] = cur
	}

	var zero uint256.Int
	for _, p := range postings {
		var amt uint256.Int
		p.GetAmount().IntoUint256(&amt)
		asset := p.GetAsset()
		bump(VolumeKey{Address: p.GetSource(), Asset: asset}, &zero, &amt)
		bump(VolumeKey{Address: p.GetDestination(), Asset: asset}, &amt, &zero)
	}

	return pcv
}

// applyAddMetadata predicts a SaveMetadata, dispatching on the target. Metadata
// lives outside the volume table, so it never touches the transient/purge
// write-set.
func (s *LedgerState) applyAddMetadata(cmd *commonpb.SaveMetadataCommand) OrderResult {
	switch t := cmd.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		return s.applyAddAccountMetadata(t.Account.GetAddr(), cmd.GetMetadata())
	case *commonpb.Target_TransactionId:
		return s.applyAddTxMetadata(t.TransactionId, cmd.GetMetadata())
	default:
		panic(fmt.Sprintf("model: AddMetadata target %T is unmodeled", cmd.GetTarget().GetTarget()))
	}
}

// applyAddAccountMetadata sets account metadata last-writer-wins, under STRICT
// chart enforcement on the address (same as a transaction posting).
func (s *LedgerState) applyAddAccountMetadata(addr string, md map[string]*commonpb.MetadataValue) OrderResult {
	compiled := s.compiled()
	if len(compiled) > 0 && addr != "world" && s.match(addr, compiled) == nil {
		return OrderResult{Reason: domain.ErrReasonAccountNotMatchingType}
	}

	saved := make(map[string]*commonpb.MetadataValue, len(md))

	for key, val := range md {
		mk := MetaKey{Address: addr, Key: key}
		s.metadata[mk] = val
		saved[key] = val
	}

	return OrderResult{OK: true, Meta: &metaEffect{saved: saved}}
}

// applyAddTxMetadata sets transaction metadata last-writer-wins on a transaction
// addressed by id. An unknown id rejects with TRANSACTION_NOT_FOUND.
func (s *LedgerState) applyAddTxMetadata(id uint64, md map[string]*commonpb.MetadataValue) OrderResult {
	ref, ok := s.txIDToRef[id]
	if !ok {
		return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
	}

	saved := make(map[string]*commonpb.MetadataValue, len(md))

	for key, val := range md {
		tk := TxMetaKey{Reference: ref, Key: key}
		s.txMeta[tk] = val
		saved[key] = val
	}

	return OrderResult{OK: true, Meta: &metaEffect{saved: saved}}
}

// applyDeleteMetadata predicts a DeleteMetadata, dispatching on the target.
// Deleting a key the entity doesn't carry rejects with METADATA_NOT_FOUND; an
// unknown transaction id rejects with TRANSACTION_NOT_FOUND.
func (s *LedgerState) applyDeleteMetadata(cmd *commonpb.DeleteMetadataCommand) OrderResult {
	switch t := cmd.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		mk := MetaKey{Address: t.Account.GetAddr(), Key: cmd.GetKey()}
		if _, exists := s.metadata[mk]; !exists {
			return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
		}

		delete(s.metadata, mk)

		return OrderResult{OK: true}
	case *commonpb.Target_TransactionId:
		ref, ok := s.txIDToRef[t.TransactionId]
		if !ok {
			return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
		}

		tk := TxMetaKey{Reference: ref, Key: cmd.GetKey()}
		if _, exists := s.txMeta[tk]; !exists {
			return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
		}

		delete(s.txMeta, tk)

		return OrderResult{OK: true}
	default:
		panic(fmt.Sprintf("model: DeleteMetadata target %T is unmodeled", cmd.GetTarget().GetTarget()))
	}
}

// applySaveLedgerMetadata predicts a SaveLedgerMetadata: a last-writer-wins set of
// each key into the ledger's own metadata. Ledger metadata is keyed only by key
// (no account), so there is no chart enforcement.
func (s *LedgerState) applySaveLedgerMetadata(req *servicepb.SaveLedgerMetadataRequest) OrderResult {
	saved := make(map[string]*commonpb.MetadataValue, len(req.GetMetadata()))

	for key, val := range req.GetMetadata() {
		s.ledgerMeta[key] = val
		saved[key] = val
	}

	return OrderResult{OK: true, Meta: &metaEffect{saved: saved}}
}

// applyDeleteLedgerMetadata predicts a DeleteLedgerMetadata: deleting a key the
// ledger doesn't carry rejects the bulk with METADATA_NOT_FOUND.
func (s *LedgerState) applyDeleteLedgerMetadata(req *servicepb.DeleteLedgerMetadataRequest) OrderResult {
	key := req.GetKey()
	if _, exists := s.ledgerMeta[key]; !exists {
		return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
	}

	delete(s.ledgerMeta, key)

	return OrderResult{OK: true}
}

// applySetMetadataFieldType declares (or re-declares) a metadata field's type.
//
// Stored values are immutable: declaring a type only records the declared type
// and never rewrites stored values. The declared type is applied at read time, so
// a value survives any retype chain losslessly (a STRING "01" retyped INT64 then
// back to STRING still reads "01"). Always succeeds.
func (s *LedgerState) applySetMetadataFieldType(req *servicepb.SetMetadataFieldTypeRequest) OrderResult {
	switch req.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		s.accountFieldTypes[req.GetKey()] = req.GetType()
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		s.ledgerFieldTypes[req.GetKey()] = req.GetType()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		s.transactionFieldTypes[req.GetKey()] = req.GetType()
	default:
		panic(fmt.Sprintf("model: SetMetadataFieldType target %v is unmodeled", req.GetTargetType()))
	}

	return OrderResult{OK: true}
}

// applyRemoveMetadataFieldType drops a field's declared type. Stored values are
// untouched; without a declared type, reads no longer coerce the key. Removing an
// undeclared field is a no-op, matching the server. Always succeeds.
func (s *LedgerState) applyRemoveMetadataFieldType(req *servicepb.RemoveMetadataFieldTypeRequest) OrderResult {
	switch req.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		delete(s.accountFieldTypes, req.GetKey())
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		delete(s.ledgerFieldTypes, req.GetKey())
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		delete(s.transactionFieldTypes, req.GetKey())
	default:
		panic(fmt.Sprintf("model: RemoveMetadataFieldType target %v is unmodeled", req.GetTargetType()))
	}

	return OrderResult{OK: true}
}

// transientViolation reports whether any touched cell matching a TRANSIENT type
// is left non-zero — the server rejects the whole bulk with
// TRANSIENT_ACCOUNT_NON_ZERO in that case. base is the pre-bulk state: an
// account that already had a non-zero balance before this bulk is grandfathered
// (it had volumes before being marked transient) and exempt — mirroring the
// server's ValidateTransientVolumes.
func (s *LedgerState) transientViolation(base *LedgerState, touched map[VolumeKey]bool) (string, bool) {
	compiled := s.compiled()
	for key := range touched {
		vp, ok := s.volumes[key]
		if !ok {
			continue
		}

		t := s.match(key.Address, compiled)
		if t == nil || t.Persistence != commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			continue
		}

		// Grandfather clause: pre-existing non-zero balance is exempt.
		bv := base.vol(key)
		if bv.Input.Cmp(&bv.Output) != 0 {
			continue
		}

		if vp.Input.Cmp(&vp.Output) != 0 {
			return domain.ErrReasonTransientAccountNonZero, true
		}
	}

	return "", false
}

// purgeZeroBalance drops touched EPHEMERAL/TRANSIENT cells that landed at a zero
// balance, mirroring the server's post-commit write-set sweep (PR #151).
func (s *LedgerState) purgeZeroBalance(touched map[VolumeKey]bool) {
	compiled := s.compiled()
	for key := range touched {
		vp, ok := s.volumes[key]
		if !ok {
			continue
		}

		t := s.match(key.Address, compiled)
		if t == nil {
			continue
		}

		switch t.Persistence {
		case commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
			if vp.Input.Cmp(&vp.Output) == 0 {
				delete(s.volumes, key)
			}
		}
	}
}
