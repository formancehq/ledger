package oracle

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
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
// per-cell volumes. Every field is a persistent, fingerprinted collection (see
// pmap.go): a mutation rebinds the field to a new value sharing structure with
// the old, so the checker forks a state across hypothesized serializations by
// plain struct copy — forks never alias.
type LedgerState struct {
	types      Map[string, TypeState]
	volumes    Map[VolumeKey, VolumePair]
	metadata   Map[MetaKey, *commonpb.MetadataValue]
	ledgerMeta Map[string, *commonpb.MetadataValue]
	// Declared metadata field types per key, driving value coercion. Keyed by
	// metadata key (the schema is per (target, key), not per address).
	accountFieldTypes Map[string, commonpb.MetadataType]
	ledgerFieldTypes  Map[string, commonpb.MetadataType]

	// txs is the transaction log: index i holds the transaction with id i+1, so
	// ids are dense and sequential, mirroring the server (first id is 1). Every
	// committed create is appended — referenced and unreferenced alike (drains,
	// transients, and reverts). The next id is txs.Len()+1. Records are replaced,
	// never mutated in place, so forks share the pointers and the fingerprint
	// terms stay valid.
	txs List[*txRecord]
	// txByRef indexes referenced transactions by reference -> id, for the
	// generator (which targets by reference) and reference-keyed metadata writes.
	txByRef               Map[string, int]
	transactionFieldTypes Map[string, commonpb.MetadataType]

	// indexes tracks the read-store indexes the ledger has, keyed by canonical
	// IndexID string. The value is the readiness flag: false = ambiguous (created,
	// backfill may still be BUILDING on some replica — a not-ready error is
	// tolerated), true = active (confirmed READY on all replicas — results are
	// required). CreateIndex adds an ambiguous entry; the driver's readiness
	// poller flips it to active; DropIndex removes it (instantaneous under the
	// MinLogSequence pin).
	indexes Map[string, bool]

	// logs is the ledger's log stream: index i holds the log with ledger-local
	// id i+1, dense from 1, mirroring the server's LedgerBoundaries.NextLogId
	// (initialised to 1 at CreateLedger, so the first apply lands on 1). Every
	// committed ledger-scoped order appends exactly one entry; a rejected one
	// appends none, since the workload never opts into skippable_reasons —
	// which is what makes a skip consume a log id (processor.go's skip branch)
	// without committing anything.
	logs List[*logRecord]

	// retypeWindows holds, per canonical metadata-index ID, the SET of declared
	// types the index's served version may still be bound to while retype
	// rewrites converge (EN-1724), encoded as a bitmask over MetadataType. The
	// schema flips at commit — fieldTypes above hold the new type — but each
	// replica serves its current version's bound type until that rewrite's
	// atomic switch, and a CHAIN of retypes advances the replica through every
	// intermediate binding: each switch briefly serves the superseded target
	// before the next rewrite lands. A query during the window is therefore
	// legal under any accumulated type, each as a whole window. A
	// SetMetadataFieldType on an indexed key adds the type it supersedes;
	// closed by the driver once every replica provably switched with no
	// rewrite pending; dies with the index on removal.
	retypeWindows Map[string, uint32]

	// everAsset is the account-by-asset index projection: the set of
	// (account, assetBase, precision) any committed, non-excluded posting has ever
	// touched, on either side. This is the exact set the has-asset filter serves
	// (see recordAssetTouches) — a monotonic history, NOT the current volume set:
	// an account drained to zero and purged from the volume table stays here.
	everAsset Map[assetTouch, struct{}]

	// compiledChart memoizes compiled() for the current types value — nil means
	// not yet compiled (an empty chart compiles to a non-nil empty slice). Every
	// types rebind must reset it to nil. Derived state: never mutated in place
	// (only replaced), so forks may share it, and it is no part of the state's
	// identity (collections/Fingerprint exclude it).
	compiledChart []accounttype.CompiledType
}

// assetTouch identifies one account's historical touch of an (assetBase,
// precision) pair — a member of the account-by-asset index the has-asset filter
// reads. precision is uint32 to match AccountHasAssetCondition.
type assetTouch struct {
	address   string
	base      string
	precision uint32
}

func NewLedgerState() LedgerState {
	return LedgerState{
		types:             NewMap[string, TypeState](stringComparer{}, typeTerm),
		volumes:           NewMap[VolumeKey, VolumePair](volumeKeyComparer{}, volumeTerm),
		metadata:          NewMap[MetaKey, *commonpb.MetadataValue](metaKeyComparer{}, accountMetaTerm),
		ledgerMeta:        NewMap[string, *commonpb.MetadataValue](stringComparer{}, ledgerMetaTerm),
		accountFieldTypes: NewMap[string, commonpb.MetadataType](stringComparer{}, fieldTypeTerm("AF")),
		ledgerFieldTypes:  NewMap[string, commonpb.MetadataType](stringComparer{}, fieldTypeTerm("LF")),

		txs:                   NewList[*txRecord](txTerm),
		txByRef:               NewMap[string, int](stringComparer{}, txRefTerm),
		transactionFieldTypes: NewMap[string, commonpb.MetadataType](stringComparer{}, fieldTypeTerm("TF")),

		indexes:       NewMap[string, bool](stringComparer{}, indexTerm),
		retypeWindows: NewMap[string, uint32](stringComparer{}, retypeWindowTerm),
		logs:          NewList[*logRecord](logTerm),
		everAsset:     NewMap[assetTouch, struct{}](assetTouchComparer{}, assetTouchTerm),
	}
}

// collections lists every fingerprinted collection a LedgerState carries —
// the single source Fingerprint and IsEmpty derive from, so neither can fall
// behind the struct's fields. txByRef is excluded: it is an index derived from
// txs, whose fingerprint already covers the (reference, id) pairs.
func (s *LedgerState) collections() []interface {
	Fingerprint() Digest
	Len() int
} {
	return []interface {
		Fingerprint() Digest
		Len() int
	}{
		s.types, s.volumes, s.metadata, s.ledgerMeta,
		s.accountFieldTypes, s.ledgerFieldTypes, s.transactionFieldTypes,
		s.txs, s.indexes, s.everAsset, s.logs, s.retypeWindows,
	}
}

// Fingerprint is the ledger state's 128-bit identity: a hash over its
// collections' fingerprints in fixed field order.
func (s LedgerState) Fingerprint() Digest {
	t := newTerm("ledger-state")
	for _, c := range s.collections() {
		t.digest(c.Fingerprint())
	}

	return t.sum()
}

// IsEmpty reports whether the state holds nothing — the identity of a
// fresh NewLedgerState.
func (s LedgerState) IsEmpty() bool {
	for _, c := range s.collections() {
		if c.Len() > 0 {
			return false
		}
	}

	return true
}

// compiled compiles the current chart into the server's matcher form, memoized
// until the next chart op (which resets compiledChart — a chart op earlier in
// the same bulk must recompile).
func (s *LedgerState) compiled() []accounttype.CompiledType {
	if s.compiledChart == nil {
		pb := make(map[string]*commonpb.AccountType, s.types.Len())
		for name, t := range s.types.All() {
			pb[name] = &commonpb.AccountType{Name: t.Name, Pattern: t.Pattern}
		}

		s.compiledChart = accounttype.CompileTypes(pb)
	}

	return s.compiledChart
}

// match resolves addr to the type the server would pick (highest specificity,
// etc. — delegated to accounttype), or nil. compiled is passed in so a caller
// validating several addresses compiles the chart once.
func (s *LedgerState) match(addr string, compiled []accounttype.CompiledType) *TypeState {
	best := accounttype.FindMatchingType(addr, compiled)
	if best == nil {
		return nil
	}

	t, _ := s.types.Get(best.GetName())

	return &t
}

// Per-entry fingerprint terms. These are the model's canonical entry
// identities: two collections hold the same state exactly when their entries'
// terms form the same multiset (see pmap.go). Each starts with a distinct
// domain tag so entries of different collections can never collide.

func typeTerm(name string, ts TypeState) Digest {
	t := newTerm("T")
	t.str(name, ts.Name, ts.Pattern)
	t.u64(uint64(ts.Persistence))

	return t.sum()
}

func volumeTerm(k VolumeKey, v VolumePair) Digest {
	t := newTerm("V")
	t.str(k.Address, k.Asset)
	t.u256(&v.Input)
	t.u256(&v.Output)

	return t.sum()
}

func accountMetaTerm(k MetaKey, v *commonpb.MetadataValue) Digest {
	t := newTerm("M")
	t.str(k.Address, k.Key, MetaValueString(v))

	return t.sum()
}

func ledgerMetaTerm(k string, v *commonpb.MetadataValue) Digest {
	t := newTerm("LM")
	t.str(k, MetaValueString(v))

	return t.sum()
}

// fieldTypeTerm builds the term function for one field-type table; the tag
// keeps the account/ledger/transaction tables in disjoint term spaces.
func fieldTypeTerm(tag string) func(string, commonpb.MetadataType) Digest {
	return func(k string, mt commonpb.MetadataType) Digest {
		t := newTerm(tag)
		t.str(k)
		t.u64(uint64(mt))

		return t.sum()
	}
}

// indexTerm fingerprints one index-registry entry. Readiness is part of the
// identity: two bases differing only in whether an index exists or is active
// predict different query outcomes, so they must not dedup.
func indexTerm(canonical string, active bool) Digest {
	t := newTerm("IX")
	t.str(canonical)
	t.boolean(active)

	return t.sum()
}

// retypeWindowTerm fingerprints one open retype window. Part of the identity:
// bases differing only in whether a window is open — or in which types it has
// accumulated — accept different query outcomes, so they must not dedup.
func retypeWindowTerm(canonical string, typeMask uint32) Digest {
	t := newTerm("RW")
	t.str(canonical)
	t.u64(uint64(typeMask))

	return t.sum()
}

// assetTouchTerm fingerprints one ever-touched (account, assetBase, precision)
// member. Derivable from the tx log + chart, but two partial candidate-base
// foldings can reach equal logs with a different ever-touched set (types added
// at different points change per-order purge exclusion), so the set carries
// its own terms to keep dedup from collapsing bases that predict different
// has-asset outcomes.
func assetTouchTerm(k assetTouch, _ struct{}) Digest {
	t := newTerm("EA")
	t.str(k.address, k.base)
	t.u64(uint64(k.precision))

	return t.sum()
}

type assetTouchComparer struct{}

func (assetTouchComparer) Compare(a, b assetTouch) int {
	if c := strings.Compare(a.address, b.address); c != 0 {
		return c
	}

	if c := strings.Compare(a.base, b.base); c != 0 {
		return c
	}

	switch {
	case a.precision < b.precision:
		return -1
	case a.precision > b.precision:
		return 1
	default:
		return 0
	}
}

func txRefTerm(ref string, id int) Digest {
	t := newTerm("R")
	t.str(ref)
	t.u64(uint64(id))

	return t.sum()
}

// txTerm fingerprints one log entry: the tx's identity (id, reference,
// reverted, timestamp, revert relationships), postings, and metadata.
// Postings and timestamp belong in the fingerprint because two commuting
// unreferenced transactions can reach identical volumes and metadata under
// opposite serializations while differing only in which id holds which
// postings, or (for at-effective-date reverts) in the inherited timestamp —
// distinctions validateTransactionRead checks by id. The revert
// relationships (revertedBy, revertsTransaction, revertedAt) distinguish
// serializations where the same id is reverted by, or reverts, a different
// transaction.
// logRecord is one entry of the ledger log stream. The id is DERIVED, never
// taken from the server: the stream is dense from 1, so the model can predict
// it, and predicting it is what lets a mis-assigned id be caught at all. Only
// the date is learned from the commit response (LearnLogDate) — it is
// server-assigned with no derivable value, and nil means not yet learned.
type logRecord struct {
	id   uint64
	kind string
	date *commonpb.Timestamp
}

func logTerm(idx int, l *logRecord) Digest {
	t := newTerm("LOG")
	t.u64(uint64(idx), l.id)
	t.str(l.kind)

	// A nil date (not yet learned) must not collide with any concrete value.
	t.boolean(l.date != nil)
	t.u64(l.date.GetData())

	return t.sum()
}

func txTerm(idx int, tx *txRecord) Digest {
	t := newTerm("TX")
	t.u64(uint64(idx), tx.id)
	t.str(tx.reference)
	t.boolean(tx.reverted)
	t.u64(tx.revertedBy, tx.revertsTransaction)

	// A nil timestamp (server-dated, unpredictable) must not collide with any
	// concrete value: validateTransactionRead skips the check only when nil.
	// Same for revertedAt.
	t.boolean(tx.timestamp != nil)
	t.u64(tx.timestamp.GetData())
	t.boolean(tx.insertedAt != nil)
	t.u64(tx.insertedAt.GetData())
	t.boolean(tx.revertedAt != nil)
	t.u64(tx.revertedAt.GetData())

	// indexedAddrs is derived at end of bulk, but two partial foldings can
	// stamp the same postings differently (exclusion depends on the chart and
	// balances at the stamping bulk's end), and the stamps drive address-query
	// predictions — so they are part of the record's identity.
	addrs := make([]string, 0, len(tx.indexedAddrs))
	for a := range tx.indexedAddrs {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	t.u64(uint64(len(addrs)))
	for _, a := range addrs {
		t.str(a)
		t.u64(uint64(tx.indexedAddrs[a]))
	}

	var amt uint256.Int
	t.u64(uint64(len(tx.postings)))
	for _, p := range tx.postings {
		p.GetAmount().IntoUint256(&amt)
		t.str(p.GetSource(), p.GetDestination(), p.GetAsset())
		t.u256(&amt)
	}

	keys := make([]string, 0, len(tx.metadata))
	for k := range tx.metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.u64(uint64(len(keys)))
	for _, k := range keys {
		t.str(k, MetaValueString(tx.metadata[k]))
	}

	return t.sum()
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
	case *commonpb.MetadataValue_DatetimeValue:
		return "d:" + strconv.FormatInt(t.DatetimeValue, 10)
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
	v, _ := s.volumes.Get(key)

	return v
}

// accountMetadata returns addr's metadata as a key→value map (empty if none).
func (s *LedgerState) AccountMetadata(addr string) map[string]*commonpb.MetadataValue {
	out := map[string]*commonpb.MetadataValue{}
	for mk, v := range s.metadata.All() {
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
	// idempotency freezes the committed outcome of every keyed bulk, so a later
	// bulk carrying the same key replays it (Apply). It spans ledgers because a
	// bulk's key covers the whole atomic batch, whatever ledgers it touched.
	// Entries are immutable once frozen (infinite TTL — the model never evicts),
	// so forks share the pointers.
	idempotency Map[string, *frozenOutcome]
}

// frozenOutcome is a keyed bulk's recorded outcome: the exact requests it
// committed (to tell a genuine replay from a same-key/different-body conflict)
// and the per-order results the server will echo on every replay.
type frozenOutcome struct {
	requests []*servicepb.Request
	orders   []OrderResult
}

// frozenOutcomeTerm fingerprints one frozen entry. Frozen idempotency outcomes
// are observable through a replay, so bases that differ only in which id a key
// froze must stay distinct — otherwise candidateBases collapses two commuting
// keyed transactions (identical business state, opposite id assignments) and a
// replay can no longer resolve to the id the server actually returned. Only the
// outcome identity a replay reveals (per-order id) needs fingerprinting, not
// the whole result.
func frozenOutcomeTerm(key string, fo *frozenOutcome) Digest {
	t := newTerm("IK")
	t.str(key)
	t.u64(uint64(len(fo.orders)))
	for i, o := range fo.orders {
		revertedID := uint64(0)
		if o.Revert != nil {
			revertedID = o.Revert.revertedID
		}
		t.u64(uint64(i), o.TxID, revertedID)
	}

	return t.sum()
}

func NewGlobalState() GlobalState {
	return GlobalState{
		ledgers:     map[string]LedgerState{},
		idempotency: NewMap[string, *frozenOutcome](stringComparer{}, frozenOutcomeTerm),
	}
}

// clone returns a copy with its own ledgers table. A LedgerState is a value of
// persistent collections, so the shallow copy is a full logical fork — forks
// never share mutable state. The idempotency table is itself persistent and
// carries over by value.
func (g GlobalState) clone() GlobalState {
	m := make(map[string]LedgerState, len(g.ledgers))
	maps.Copy(m, g.ledgers)

	return GlobalState{ledgers: m, idempotency: g.idempotency}
}

// ledger returns the named ledger's state, or an empty one if untouched.
func (g GlobalState) Ledger(name string) LedgerState {
	if ls, ok := g.ledgers[name]; ok {
		return ls
	}

	return NewLedgerState()
}

// Fingerprint is the state's 128-bit identity across all non-empty ledgers:
// the multiset sum of one term per ledger, so ledger order is irrelevant.
// candidateBases dedups on it, collapsing bases reached via different
// (commutative) serializations. Empty ledgers are skipped: Apply materializes
// a ledger entry for any ledger a bulk touches, even when the operation stores
// nothing (e.g. removing an undeclared field), and a present-but-stateless
// entry must not change the identity — otherwise candidateBases treats
// semantically-equal bases as distinct.
func (g GlobalState) Fingerprint() Digest {
	var d Digest
	for name, ls := range g.ledgers {
		if ls.IsEmpty() {
			continue
		}

		t := newTerm("L")
		t.str(name)
		t.digest(ls.Fingerprint())
		d = d.add(t.sum())
	}

	// The frozen idempotency table is part of the identity — see
	// frozenOutcomeTerm. Its terms are domain-tagged, so the plain sum keeps
	// them disjoint from the ledger terms.
	return d.add(g.idempotency.Fingerprint())
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

// txRecord is a committed transaction in the log: its server-assigned id, its
// reference ("" for drains, transients, and reverts), its postings, its metadata
// (set at creation and by later metadata writes), and whether it has been
// reverted (a second revert is rejected). Records are replaced, never mutated in
// place, so clones safely share the pointer.
type txRecord struct {
	id        uint64
	reference string
	postings  []*commonpb.Posting
	metadata  map[string]*commonpb.MetadataValue
	reverted  bool
	// timestamp is the user-supplied CreateTransaction timestamp, stored verbatim
	// and echoed on reads. nil when the client sent none — the server then stamps
	// its own command date, which the model cannot predict, so reads skip the
	// timestamp check for such records. The checker may later fill a nil via
	// LearnTxStamps with the value the commit response carried.
	timestamp *commonpb.Timestamp
	// insertedAt is always server-stamped (never client-supplied), so it starts
	// nil and is known only once the checker learns it from the commit response.
	insertedAt *commonpb.Timestamp
	// Revert relationships, mirroring the server's Transaction fields: on a
	// reverted original, revertedBy carries the compensating transaction's id
	// and revertedAt its timestamp (nil when the compensating transaction is
	// server-dated — unpredictable, so reads skip it, like timestamp); on a
	// revert transaction, revertsTransaction carries the original's id. Zero
	// values mean not reverted / not a revert.
	revertedBy         uint64
	revertedAt         *commonpb.Timestamp
	revertsTransaction uint64
	// indexedAddrs is the transaction's account→tx index membership: per
	// posting-side account, which role rows (AddrIndexedSource /
	// AddrIndexedDestination bits) the index builder writes for this
	// transaction. Stamped at end of bulk (recordIndexedAddrs) — a posting side
	// whose cell lands in the exclusion projection gets no row. The any-role
	// index is the union of the two bits.
	indexedAddrs map[string]uint8
}

// Bits of txRecord.indexedAddrs / IndexedAddrs.
const (
	AddrIndexedSource      uint8 = 1 << 0
	AddrIndexedDestination uint8 = 1 << 1
)

// revertEffect is a committed revert's predicted effect: the original
// transaction id (echoed as reverted_transaction_id) and the reversed postings.
// The revert transaction's own metadata is verified through a read of its log
// entry, not here.
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
	case *servicepb.Request_CreateIndex:
		return r.CreateIndex.GetLedger()
	case *servicepb.Request_DropIndex:
		return r.DropIndex.GetLedger()
	default:
		panic(fmt.Sprintf("LedgerOf: unmodeled request type %T", req.GetType()))
	}
}

// Apply folds bulk's requests into g in order, predicting each one. The server
// applies a bulk atomically (one Raft entry): the first failing request — or an
// end-of-bulk transient violation on any touched ledger — rejects the whole bulk
// and leaves every ledger unchanged. A bulk may span ledgers; each request is
// routed to its own ledger's sub-state and the end-of-bulk checks run per ledger.
// SeedInitialSchema declares a ledger's metadata field types the way
// CreateLedger's initial_schema does: state only, no log.
//
// The distinction is load-bearing. Replaying the declarations as
// SetMetadataFieldType requests through Apply reaches the same schema state,
// but on the server those types are recorded at creation and never processed
// as apply orders — they consume no ledger-local log id and never appear in
// ListLogs. Seeding through Apply would therefore put one phantom log at the
// head of the stream per declared field and shift every real log's id by that
// many.
func (g GlobalState) SeedInitialSchema(reqs []*servicepb.Request) GlobalState {
	next := g.clone()

	for _, req := range reqs {
		name := LedgerOf(req)

		ls, ok := next.ledgers[name]
		if !ok {
			ls = NewLedgerState()
		}

		ls.applyOne(req, map[VolumeKey]bool{})
		next.ledgers[name] = ls
	}

	return next
}

func (g GlobalState) Apply(bulk Bulk) ApplyResult {
	// Admission validates every order's structure and converts the whole batch
	// before it reaches the FSM, so a single malformed order rejects the entire
	// bulk ahead of any per-order FSM outcome. The only structural rejection the
	// workload produces is an empty create (no postings, no script → VALIDATION);
	// model it here so a bulk mixing an empty create with an FSM-rejecting order
	// reports VALIDATION, matching validateOrderContent rather than the FSM reason
	// the sequential pass below would reach first.
	for _, req := range bulk.Requests {
		if ct := req.GetApply().GetAction().GetCreateTransaction(); ct != nil && len(ct.GetPostings()) == 0 {
			return ApplyResult{OK: false, Reason: domain.ErrReasonValidation, State: g}
		}
	}

	// Per-batch idempotency, checked after admission's structural gate (the
	// empty-create above) and before any FSM outcome — mirroring the server,
	// where the dedup runs in the apply path ahead of ProcessOrders. Only
	// successes are frozen (see the commit return below), so a hit is always a
	// committed outcome: same body replays it verbatim with no new state; a
	// different body under the same key is a conflict.
	if bulk.IdempotencyKey != "" {
		if fo, ok := g.idempotency.Get(bulk.IdempotencyKey); ok {
			if !RequestsEqual(fo.requests, bulk.Requests) {
				return ApplyResult{OK: false, Reason: domain.ErrReasonIdempotencyKeyConflict, State: g}
			}

			return ApplyResult{OK: true, State: g, Orders: fo.orders}
		}
	}

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
		if oc.OK {
			// Appended centrally rather than per handler: every committed
			// ledger-scoped order produces exactly one log, so a handler that
			// forgot would silently shorten the stream and mis-id every log
			// after it.
			ls.appendLog(req)
		}

		// applyOne rebinds ls's persistent collections; write the updated value
		// back so the working copy sees the mutation.
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

		ls.recordAssetTouches(&base, cells)
		ls.recordIndexedAddrs(&base, uint64(base.Txs().Len())+1)
		ls.purgeZeroBalance(cells)
		next.ledgers[name] = ls
	}

	// Freeze the committed outcome so a later bulk with this key replays it. Only
	// the success path records an entry; a rejected keyed bulk leaves no frozen
	// outcome (the driver never replays a key whose bulk did not commit).
	if bulk.IdempotencyKey != "" {
		next.idempotency = next.idempotency.Set(bulk.IdempotencyKey, &frozenOutcome{
			requests: bulk.Requests,
			orders:   orders,
		})
	}

	return ApplyResult{OK: true, State: next, Orders: orders}
}

// RequestsEqual reports whether two request slices are element-wise equal,
// telling a genuine replay (same body) from a same-key/different-body conflict.
// It is a faithful proxy for the server's idempotency hash: every request field
// rides on the hashed order, and only admission's OrderTechnical is excluded —
// so equal requests hash identically (replay) and any difference conflicts.
func RequestsEqual(a, b []*servicepb.Request) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !a[i].EqualVT(b[i]) {
			return false
		}
	}

	return true
}

// LogIDs returns the ledger-local ids of every committed log, ascending. The
// stream is dense from 1, so this is 1..Len() — returned explicitly so callers
// need not depend on that density holding.
func (s LedgerState) LogIDs() []uint64 {
	out := make([]uint64, 0, s.logs.Len())
	for i := range s.logs.Len() {
		out = append(out, s.logs.Get(i).id)
	}

	return out
}

// logKindFor names the LedgerLogPayload arm a committed request produces, or
// "" for a request that appends nothing to the ledger log stream. It is
// derived from the request rather than declared by each apply* handler, so a
// new request type cannot silently append a log of the wrong kind — or, if it
// is unmodelled, reach the stream at all: the default panics in lockstep with
// applyOne's own unmodelled-request panic.
//
// Only orders processed through processApply join the stream. processApply is
// what consumes LedgerBoundaries.NextLogId, and the index builder folds a log
// into the per-ledger limb ListLogs reads only when its payload is the Apply
// arm. Ledger metadata is the exception: processAddLedgerMetadata and its
// delete twin are dispatched as their own LedgerScopedOrder and return a
// top-level SavedLedgerMetadata / DeletedLedgerMetadata payload, so they take
// no ledger-local id and never appear in ListLogs.
func logKindFor(req *servicepb.Request) string {
	switch r := req.GetType().(type) {
	case *servicepb.Request_AddAccountType:
		return "added_account_type"
	case *servicepb.Request_RemoveAccountType:
		return "removed_account_type"
	case *servicepb.Request_SaveLedgerMetadata, *servicepb.Request_DeleteLedgerMetadata:
		return ""
	case *servicepb.Request_SetMetadataFieldType:
		return "set_metadata_field_type"
	case *servicepb.Request_RemoveMetadataFieldType:
		return "removed_metadata_field_type"
	case *servicepb.Request_CreateIndex:
		return "create_index"
	case *servicepb.Request_DropIndex:
		return "drop_index"
	case *servicepb.Request_Apply:
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return "created_transaction"
		case *servicepb.LedgerAction_AddMetadata:
			return "saved_metadata"
		case *servicepb.LedgerAction_DeleteMetadata:
			return "deleted_metadata"
		case *servicepb.LedgerAction_RevertTransaction:
			return "reverted_transaction"
		}
	}

	panic(fmt.Sprintf("model: no log kind for request %T", req.GetType()))
}

// appendLog records the log a committed request produced, if it produced one.
// The id is the stream's position, dense from 1.
func (s *LedgerState) appendLog(req *servicepb.Request) {
	kind := logKindFor(req)
	if kind == "" {
		return
	}

	s.logs = s.logs.Append(&logRecord{id: uint64(s.logs.Len()) + 1, kind: kind})
}

// applyOne mutates the (already-forked) working state for one request and
// returns its predicted outcome, recording touched volume cells.
func (s *LedgerState) applyOne(req *servicepb.Request, touched map[VolumeKey]bool) OrderResult {
	switch r := req.GetType().(type) {
	case *servicepb.Request_AddAccountType:
		at := r.AddAccountType.GetAccountType()
		name := at.GetName()
		if s.types.Has(name) {
			return OrderResult{Reason: domain.ErrReasonAccountTypeAlreadyExists}
		}

		s.types = s.types.Set(name, TypeState{Name: name, Pattern: at.GetPattern(), Persistence: at.GetPersistence()})
		s.compiledChart = nil

		return OrderResult{OK: true}

	case *servicepb.Request_RemoveAccountType:
		name := r.RemoveAccountType.GetName()
		if !s.types.Has(name) {
			return OrderResult{Reason: domain.ErrReasonAccountTypeNotFound}
		}

		s.types = s.types.Delete(name)
		s.compiledChart = nil

		return OrderResult{OK: true}

	case *servicepb.Request_SaveLedgerMetadata:
		return s.applySaveLedgerMetadata(r.SaveLedgerMetadata)

	case *servicepb.Request_DeleteLedgerMetadata:
		return s.applyDeleteLedgerMetadata(r.DeleteLedgerMetadata)

	case *servicepb.Request_SetMetadataFieldType:
		return s.applySetMetadataFieldType(r.SetMetadataFieldType)

	case *servicepb.Request_RemoveMetadataFieldType:
		return s.applyRemoveMetadataFieldType(r.RemoveMetadataFieldType)

	case *servicepb.Request_CreateIndex:
		return s.applyCreateIndex(r.CreateIndex)

	case *servicepb.Request_DropIndex:
		return s.applyDropIndex(r.DropIndex)

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

// applyTransaction predicts a CreateTransaction, matching the server's FSM
// rejection order (empty payloads are rejected earlier, at admission — see
// Apply): a duplicate reference is rejected first (processor_transaction.go,
// before produce()); then the server produces the postings — applying the
// per-posting balance floor (a non-forced debit from a non-world account may not
// exceed its running balance — see applyPostings) — BEFORE it validates account
// types (produce() then validatePostingsAgainstAccountTypes). So an underfunded
// transaction reports INSUFFICIENT_FUNDS even when an address also fails the
// chart; match that order — floor first, then STRICT chart enforcement.
func (s *LedgerState) applyTransaction(ct *servicepb.CreateTransactionPayload, touched map[VolumeKey]bool) OrderResult {
	postings := ct.GetPostings()

	// A reference must be unique; the FSM checks this first, before producing
	// postings or enforcing the chart, so a duplicate wins over any floor/chart
	// issue the same transaction might also have.
	ref := ct.GetReference()
	if ref != "" && s.txByRef.Has(ref) {
		return OrderResult{Reason: domain.ErrReasonTransactionReferenceConflict}
	}

	pcv, reason := s.applyPostings(postings, ct.GetForce(), touched)
	if reason != "" {
		return OrderResult{Reason: reason}
	}

	if s.chartRejects(postings) {
		return OrderResult{Reason: domain.ErrReasonAccountNotMatchingType}
	}

	// Account metadata attached to the transaction is applied verbatim, last-
	// writer-wins. The server applies it without chart enforcement (unlike a
	// standalone AddMetadata — processor_transaction.go); the generator only
	// attaches it to the transaction's own accounts, which already passed the
	// posting chart check above, so no enforcement branch is needed.
	for account, mm := range ct.GetAccountMetadata() {
		for key, val := range mm.GetValues() {
			s.metadata = s.metadata.Set(MetaKey{Address: account, Key: key}, val)
		}
	}

	// Append to the log; the id is its 1-based position. Metadata is stored
	// verbatim (the declared type is applied only on read) and echoed on the
	// CreatedTransaction log.
	id := uint64(s.txs.Len()) + 1
	s.txs = s.txs.Append(&txRecord{
		id:        id,
		reference: ref,
		postings:  postings,
		metadata:  ct.GetMetadata(),
		timestamp: ct.GetTimestamp(),
	})
	if ref != "" {
		s.txByRef = s.txByRef.Set(ref, int(id))
	}

	var meta *metaEffect
	if len(ct.GetMetadata()) > 0 {
		meta = &metaEffect{saved: ct.GetMetadata()}
	}

	return OrderResult{OK: true, PCV: pcv, Meta: meta, TxID: id}
}

// applyRevert predicts a RevertTransaction: it reverses the original postings
// (swap source/destination), enforces the chart on them, applies the balance
// floor unless force is set (see applyPostings), moves the volumes, marks the
// original reverted, and consumes a new transaction id for the revert itself.
func (s *LedgerState) applyRevert(rt *servicepb.RevertTransactionPayload, touched map[VolumeKey]bool) OrderResult {
	id := rt.GetTransactionId()
	if id == 0 || id > uint64(s.txs.Len()) {
		// Unknown id (past the log frontier); the server rejects with
		// TRANSACTION_NOT_FOUND. The generator targets committed transactions, so
		// in commit order this is unreachable, but a candidate-base ordering may
		// not have applied the create yet.
		return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
	}

	orig := s.txs.Get(int(id - 1))

	if orig.reverted {
		return OrderResult{Reason: domain.ErrReasonTransactionAlreadyReverted}
	}

	reversed := make([]*commonpb.Posting, len(orig.postings))
	for i, p := range orig.postings {
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

	pcv, reason := s.applyPostings(reversed, rt.GetForce(), touched)
	if reason != "" {
		return OrderResult{Reason: reason}
	}

	// A plain revert stamps the server's current date (nil here — unpredictable,
	// so reads skip it). With at_effective_date the revert inherits the original's
	// timestamp (processor_revert_transaction.go), which the model knows iff the
	// original carried a user-supplied one; otherwise it too is a server date (nil).
	var revertTS *commonpb.Timestamp
	if rt.GetAtEffectiveDate() {
		revertTS = orig.timestamp
	}

	revertID := uint64(s.txs.Len()) + 1

	// Mark the original reverted (replace, don't mutate), then append the revert
	// itself as a new unreferenced transaction carrying the reversed postings and
	// any metadata the revert set. The reverted_at stamped on the original is the
	// compensating transaction's timestamp (processor_revert_transaction.go).
	reverted := *orig
	reverted.reverted = true
	reverted.revertedBy = revertID
	reverted.revertedAt = revertTS
	s.txs = s.txs.Set(int(id-1), &reverted)

	s.txs = s.txs.Append(&txRecord{id: revertID, postings: reversed, metadata: rt.GetMetadata(), timestamp: revertTS, revertsTransaction: orig.id})

	return OrderResult{
		OK:     true,
		PCV:    pcv,
		TxID:   revertID,
		Revert: &revertEffect{revertedID: orig.id, postings: reversed},
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
// applyPostings folds postings into the running volumes in order and returns the
// per-cell post-commit volumes. A non-forced debit from a non-world account is
// held to its balance floor (input - output): if the amount exceeds it the whole
// bulk is rejected with INSUFFICIENT_FUNDS (returned reason != ""). The floor is
// evaluated against the running volumes, so an earlier posting in the same bulk
// can fund a later source — mirroring applyPosting in processor_posting.go.
func (s *LedgerState) applyPostings(postings []*commonpb.Posting, force bool, touched map[VolumeKey]bool) (map[VolumeKey]VolumePair, string) {
	pcv := map[VolumeKey]VolumePair{}
	bump := func(key VolumeKey, addIn, addOut *uint256.Int) {
		cur := s.vol(key)
		cur.Input.Add(&cur.Input, addIn)
		cur.Output.Add(&cur.Output, addOut)
		s.volumes = s.volumes.Set(key, cur)
		touched[key] = true
		pcv[key] = cur
	}

	var zero uint256.Int
	for _, p := range postings {
		var amt uint256.Int
		p.GetAmount().IntoUint256(&amt)
		asset := p.GetAsset()
		srcKey := VolumeKey{Address: p.GetSource(), Asset: asset}
		src := s.vol(srcKey)

		var sum uint256.Int
		if !force && p.GetSource() != "world" {
			if _, overflow := sum.AddOverflow(&src.Output, &amt); overflow || src.Input.Lt(&sum) {
				return pcv, domain.ErrReasonInsufficientFunds
			}
		} else if _, overflow := sum.AddOverflow(&src.Output, &amt); overflow {
			// world / force skip the floor, but the source Output still cannot
			// overflow — processor_posting.go rejects the order (#321).
			return pcv, domain.ErrReasonVolumeOverflow
		}

		// The destination Input can never overflow either.
		dstKey := VolumeKey{Address: p.GetDestination(), Asset: asset}
		dst := s.vol(dstKey)
		if _, overflow := sum.AddOverflow(&dst.Input, &amt); overflow {
			return pcv, domain.ErrReasonVolumeOverflow
		}

		bump(srcKey, &zero, &amt)
		bump(dstKey, &amt, &zero)
	}

	return pcv, ""
}

// recordAssetTouches folds this bulk's touched cells into the ever-touched
// account-by-asset set, mirroring the read-store index writer. The index
// builder walks every posting and skips cells in the exclusion projection
// (LedgerLog.PurgedVolumes ∪ AppliedProposal.TransientVolumes), which the FSM
// derives at END of bulk from the final merged volumes and the end-of-bulk
// chart (partitionVolumes). Mirrored per touched cell:
//
//   - TRANSIENT-matched, all-zero pre-bulk value: steady-state transient —
//     carried on TransientVolumes, excluded.
//   - TRANSIENT-matched, non-zero pre-bulk value (grandfathered) or
//     EPHEMERAL-matched: excluded when the bulk leaves the cell at zero
//     balance (purged), recorded otherwise.
//   - NORMAL or unmatched: always recorded.
//
// base is the pre-bulk state (the preloaded Old the server classifies on). A
// touch is permanent once recorded (the index never deletes keys), so a purge
// in a LATER bulk does not un-record it. Must run before purgeZeroBalance —
// the classification reads the pre-purge end-of-bulk balance.
func (s *LedgerState) recordAssetTouches(base *LedgerState, touched map[VolumeKey]bool) {
	compiled := s.compiled()
	for key := range touched {
		if s.cellExcluded(base, key, compiled) {
			continue
		}

		assetBase, prec := domain.ParseAssetPrecision(key.Asset)
		s.everAsset = s.everAsset.Set(assetTouch{address: key.Address, base: assetBase, precision: uint32(prec)}, struct{}{})
	}
}

// cellExcluded is the model's copy of the per-cell verdict behind the server's
// exclusion projection (LedgerLog.PurgedVolumes ∪ AppliedProposal
// .TransientVolumes), evaluated at END of bulk on the final pre-purge volumes
// with the end-of-bulk chart:
//
//   - TRANSIENT-matched, all-zero pre-bulk value: steady-state transient —
//     carried on TransientVolumes, excluded.
//   - TRANSIENT-matched, non-zero pre-bulk value (grandfathered) or
//     EPHEMERAL-matched: excluded when the bulk leaves the cell at zero
//     balance (purged), kept otherwise.
//   - NORMAL or unmatched: never excluded.
//
// base is the pre-bulk state (the preloaded Old the server classifies on). Must
// run before purgeZeroBalance — the verdict reads the pre-purge balance. A cell
// the bulk never wrote (absent from volumes) is vacuously excluded.
func (s *LedgerState) cellExcluded(base *LedgerState, key VolumeKey, compiled []accounttype.CompiledType) bool {
	vp, ok := s.volumes.Get(key)
	if !ok {
		return true
	}

	t := s.match(key.Address, compiled)
	if t == nil {
		return false
	}

	switch t.Persistence {
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT:
		bv := base.vol(key)
		if bv.Input.IsZero() && bv.Output.IsZero() {
			return true
		}

		return vp.Input.Cmp(&vp.Output) == 0
	case commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL:
		return vp.Input.Cmp(&vp.Output) == 0
	default:
		return false
	}
}

// recordIndexedAddrs stamps every transaction this bulk appended (ids in
// (firstNew-1, len(txs)]) with its account→tx index membership: for each
// posting side, the (account, role) pair is indexed unless the posting's cell
// is in the end-of-bulk exclusion projection — mirroring the index builder's
// per-posting sourceExcluded/destinationExcluded skip. The any-role index is
// the union of the two sides, so only source/destination bits are stored.
// Replacing the records is safe: they were allocated by this Apply, so no
// older clone aliases them. Must run before purgeZeroBalance, like
// recordAssetTouches.
func (s *LedgerState) recordIndexedAddrs(base *LedgerState, firstNew uint64) {
	compiled := s.compiled()

	for id := firstNew; id <= uint64(s.txs.Len()); id++ {
		rec := *s.txs.Get(int(id - 1))
		rec.indexedAddrs = map[string]uint8{}

		for _, p := range rec.postings {
			if !s.cellExcluded(base, VolumeKey{Address: p.GetSource(), Asset: p.GetAsset()}, compiled) {
				rec.indexedAddrs[p.GetSource()] |= AddrIndexedSource
			}

			if !s.cellExcluded(base, VolumeKey{Address: p.GetDestination(), Asset: p.GetAsset()}, compiled) {
				rec.indexedAddrs[p.GetDestination()] |= AddrIndexedDestination
			}
		}

		s.txs = s.txs.Set(int(id-1), &rec)
	}
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
		s.metadata = s.metadata.Set(MetaKey{Address: addr, Key: key}, val)
		saved[key] = val
	}

	return OrderResult{OK: true, Meta: &metaEffect{saved: saved}}
}

// applyAddTxMetadata sets transaction metadata last-writer-wins on a transaction
// addressed by id. An unknown id rejects with TRANSACTION_NOT_FOUND.
func (s *LedgerState) applyAddTxMetadata(id uint64, md map[string]*commonpb.MetadataValue) OrderResult {
	if id == 0 || id > uint64(s.txs.Len()) {
		return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
	}

	old := s.txs.Get(int(id - 1))
	meta := make(map[string]*commonpb.MetadataValue, len(old.metadata)+len(md))
	maps.Copy(meta, old.metadata)
	maps.Copy(meta, md) // last-writer-wins
	// Replace (don't mutate) so forks sharing the pointer are unaffected; a
	// value copy carries every field, including the revert relationships.
	updated := *old
	updated.metadata = meta
	s.txs = s.txs.Set(int(id-1), &updated)

	return OrderResult{OK: true, Meta: &metaEffect{saved: md}}
}

// applyDeleteMetadata predicts a DeleteMetadata, dispatching on the target.
// Deleting a key the entity doesn't carry rejects with METADATA_NOT_FOUND; an
// unknown transaction id rejects with TRANSACTION_NOT_FOUND.
func (s *LedgerState) applyDeleteMetadata(cmd *commonpb.DeleteMetadataCommand) OrderResult {
	switch t := cmd.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		mk := MetaKey{Address: t.Account.GetAddr(), Key: cmd.GetKey()}
		if !s.metadata.Has(mk) {
			return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
		}

		s.metadata = s.metadata.Delete(mk)

		return OrderResult{OK: true}
	case *commonpb.Target_TransactionId:
		id := t.TransactionId
		if id == 0 || id > uint64(s.txs.Len()) {
			return OrderResult{Reason: domain.ErrReasonTransactionNotFound}
		}

		old := s.txs.Get(int(id - 1))
		if _, exists := old.metadata[cmd.GetKey()]; !exists {
			return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
		}

		meta := make(map[string]*commonpb.MetadataValue, len(old.metadata))
		maps.Copy(meta, old.metadata)
		delete(meta, cmd.GetKey())
		// Replace (don't mutate) so forks sharing the pointer are unaffected; a
		// value copy carries every field, including the revert relationships.
		updated := *old
		updated.metadata = meta
		s.txs = s.txs.Set(int(id-1), &updated)

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
		s.ledgerMeta = s.ledgerMeta.Set(key, val)
		saved[key] = val
	}

	return OrderResult{OK: true, Meta: &metaEffect{saved: saved}}
}

// applyDeleteLedgerMetadata predicts a DeleteLedgerMetadata: deleting a key the
// ledger doesn't carry rejects the bulk with METADATA_NOT_FOUND.
func (s *LedgerState) applyDeleteLedgerMetadata(req *servicepb.DeleteLedgerMetadataRequest) OrderResult {
	key := req.GetKey()
	if !s.ledgerMeta.Has(key) {
		return OrderResult{Reason: domain.ErrReasonMetadataNotFound}
	}

	s.ledgerMeta = s.ledgerMeta.Delete(key)

	return OrderResult{OK: true}
}

// applyCreateIndex records a newly created index as ambiguous (readiness unknown
// until the driver's poller confirms it READY). CreateIndex is idempotent on the
// server (a duplicate on a present index is a no-op, no AlreadyExists), so an
// existing entry keeps its current readiness flag.
func (s *LedgerState) applyCreateIndex(req *servicepb.CreateIndexRequest) OrderResult {
	// A metadata index targets a declared schema field; creating one for an
	// undeclared (target, key) is rejected (validateIndexTarget).
	if meta, ok := req.GetId().GetKind().(*commonpb.IndexID_Metadata); ok {
		if !s.fieldTypes(meta.Metadata.GetTarget()).Has(meta.Metadata.GetKey()) {
			return OrderResult{Reason: domain.ErrReasonMetadataFieldNotInSchema}
		}
	}

	canonical := indexes.Canonical(req.GetId())
	if !s.indexes.Has(canonical) {
		s.indexes = s.indexes.Set(canonical, false) // ambiguous: created, readiness not yet confirmed
	}

	return OrderResult{OK: true}
}

// fieldTypes returns the declared-type map for a metadata target. Ledger-target
// declarations exist but have no index surface; unknown targets are the empty
// map (the caller treats every key as undeclared).
func (s *LedgerState) fieldTypes(target commonpb.TargetType) Map[string, commonpb.MetadataType] {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return s.accountFieldTypes
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		return s.ledgerFieldTypes
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return s.transactionFieldTypes
	default:
		return NewMap[string, commonpb.MetadataType](stringComparer{}, fieldTypeTerm("XF"))
	}
}

// applyDropIndex removes an index. Drop is instantaneous: once this order is in
// the committed prefix, a MinLogSequence-pinned read is guaranteed to observe it
// gone. Dropping an absent index is a harmless no-op.
func (s *LedgerState) applyDropIndex(req *servicepb.DropIndexRequest) OrderResult {
	canonical := indexes.Canonical(req.GetId())
	s.indexes = s.indexes.Delete(canonical)
	s.retypeWindows = s.retypeWindows.Delete(canonical)

	return OrderResult{OK: true}
}

// applySetMetadataFieldType declares (or re-declares) a metadata field's type.
//
// Stored values are immutable: declaring a type only records the declared type
// and never rewrites stored values. The declared type is applied at read time, so
// a value survives any retype chain losslessly (a STRING "01" retyped INT64 then
// back to STRING still reads "01"). Always succeeds.
func (s *LedgerState) applySetMetadataFieldType(req *servicepb.SetMetadataFieldTypeRequest) OrderResult {
	// A retype of an indexed key opens a serving window: the index keeps
	// answering under the type it was built with until the background rewrite
	// switches, so both types stay legal until the driver observes the switch
	// on every replica. A CHAINED retype mid-window adds the type it
	// supersedes to the window's set: each rewrite switch advances the
	// replica's served version through that intermediate binding before the
	// next rewrite lands, so a query may legally observe any of them until
	// the driver proves the chain quiescent. CreateIndex requires a declared
	// field, so the previous type always exists here. Ledger-target keys have
	// no metadata index.
	if tt := req.GetTargetType(); tt == commonpb.TargetType_TARGET_TYPE_ACCOUNT || tt == commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		canonical := indexes.Canonical(indexes.MetadataID(tt, req.GetKey()))
		if s.indexes.Has(canonical) {
			if oldType, declared := s.FieldTypeFor(tt, req.GetKey()); declared {
				mask, _ := s.retypeWindows.Get(canonical)
				s.retypeWindows = s.retypeWindows.Set(canonical, mask|1<<uint(oldType))
			}
		}
	}

	switch req.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		s.accountFieldTypes = s.accountFieldTypes.Set(req.GetKey(), req.GetType())
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		s.ledgerFieldTypes = s.ledgerFieldTypes.Set(req.GetKey(), req.GetType())
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		s.transactionFieldTypes = s.transactionFieldTypes.Set(req.GetKey(), req.GetType())
	default:
		panic(fmt.Sprintf("model: SetMetadataFieldType target %v is unmodeled", req.GetTargetType()))
	}

	return OrderResult{OK: true}
}

// applyRemoveMetadataFieldType drops a field's declared type. Stored values are
// untouched; without a declared type, reads no longer coerce the key. Removing an
// undeclared field is a no-op, matching the server. Always succeeds.
//
// A metadata index on the removed field cannot outlive its declaration — the
// server drops it in the same order (the RemovedMetadataFieldType log carries
// the DroppedIndex), so the model removes it too.
func (s *LedgerState) applyRemoveMetadataFieldType(req *servicepb.RemoveMetadataFieldTypeRequest) OrderResult {
	switch req.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		s.accountFieldTypes = s.accountFieldTypes.Delete(req.GetKey())
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		s.ledgerFieldTypes = s.ledgerFieldTypes.Delete(req.GetKey())
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		s.transactionFieldTypes = s.transactionFieldTypes.Delete(req.GetKey())
	default:
		panic(fmt.Sprintf("model: RemoveMetadataFieldType target %v is unmodeled", req.GetTargetType()))
	}

	removedCanonical := indexes.Canonical(indexes.MetadataID(req.GetTargetType(), req.GetKey()))
	s.indexes = s.indexes.Delete(removedCanonical)
	s.retypeWindows = s.retypeWindows.Delete(removedCanonical)

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
		vp, ok := s.volumes.Get(key)
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
		vp, ok := s.volumes.Get(key)
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
				s.volumes = s.volumes.Delete(key)
			}
		}
	}
}
