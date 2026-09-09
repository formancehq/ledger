package oracle

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Exported read accessors over the model's internal state. The driver and the
// replay tool inspect a committed state (chart, volumes, metadata, declared
// field types) to generate operations and compare against the SUT. The
// collections are persistent (see pmap.go), so handing them out cannot expose
// the model to mutation — every model mutation goes through Apply. Their
// iteration order is sorted, so callers may draw Antithesis-reproducible
// decisions while ranging.

func (g GlobalState) Ledgers() map[string]LedgerState { return g.ledgers }

func (s LedgerState) Types() Map[string, TypeState]                    { return s.types }
func (s LedgerState) Volumes() Map[VolumeKey, VolumePair]              { return s.volumes }
func (s LedgerState) Metadata() Map[MetaKey, *commonpb.MetadataValue]  { return s.metadata }
func (s LedgerState) LedgerMeta() Map[string, *commonpb.MetadataValue] { return s.ledgerMeta }
func (s LedgerState) AccountFieldTypes() Map[string, commonpb.MetadataType] {
	return s.accountFieldTypes
}
func (s LedgerState) LedgerFieldTypes() Map[string, commonpb.MetadataType] {
	return s.ledgerFieldTypes
}
func (s LedgerState) TransactionFieldTypes() Map[string, commonpb.MetadataType] {
	return s.transactionFieldTypes
}

// Txs is the transaction log; index i holds the transaction with id i+1. TxByRef
// indexes referenced transactions by reference -> id.
func (s LedgerState) Txs() List[*txRecord]      { return s.txs }
func (s LedgerState) TxByRef() Map[string, int] { return s.txByRef }

// txRecord accessors expose one committed transaction from the log: its
// server-assigned id, its reference ("" for drains/transients/reverts), its
// postings, its metadata, whether it has been reverted, its user-supplied
// timestamp (nil when the client sent none — see txRecord.timestamp), and its
// revert relationships (see the txRecord fields for the nil/zero conventions).
func (t *txRecord) Id() uint64                                   { return t.id }
func (t *txRecord) Reference() string                            { return t.reference }
func (t *txRecord) Postings() []*commonpb.Posting                { return t.postings }
func (t *txRecord) Metadata() map[string]*commonpb.MetadataValue { return t.metadata }
func (t *txRecord) Reverted() bool                               { return t.reverted }
func (t *txRecord) Timestamp() *commonpb.Timestamp               { return t.timestamp }
func (t *txRecord) InsertedAt() *commonpb.Timestamp              { return t.insertedAt }
func (t *txRecord) RevertedBy() uint64                           { return t.revertedBy }
func (t *txRecord) RevertedAt() *commonpb.Timestamp              { return t.revertedAt }
func (t *txRecord) RevertsTransaction() uint64                   { return t.revertsTransaction }

// IndexedAddrs is the transaction's account→tx index membership, keyed by
// account with AddrIndexedSource/AddrIndexedDestination bits — see
// txRecord.indexedAddrs. Read-only, like the other map accessors.
func (t *txRecord) IndexedAddrs() map[string]uint8 { return t.indexedAddrs }

// HasAccount reports whether the account currently holds a volume cell or a
// metadata entry — membership in the merged V+M attributes universe the
// server's address matching scans (pebbleAccountExists / the account prefix
// iterator). A purged account with no metadata is NOT in the universe even
// when older index rows still reference it.
func (s LedgerState) HasAccount(addr string) bool {
	// Each map is sorted with Address as the leading key field, so the first
	// entry at or after {addr} decides membership in that map — but a miss in
	// volumes must still fall through to the metadata scan.
	for k := range s.volumes.From(VolumeKey{Address: addr}) {
		if k.Address == addr {
			return true
		}

		break
	}

	for k := range s.metadata.From(MetaKey{Address: addr}) {
		if k.Address == addr {
			return true
		}

		break
	}

	return false
}

// Indexes returns the ledger's index set keyed by canonical IndexID (value is
// the active flag: true active, false ambiguous). Mutate index state through
// SetIndexActive / SetIndexAmbiguous.
func (s LedgerState) Indexes() Map[string, bool] { return s.indexes }

// IndexState reports whether the ledger has an index with the given canonical
// IndexID, and if so whether it is active (READY confirmed) or ambiguous
// (created, readiness not yet confirmed — a not-ready error is tolerated).
func (s LedgerState) IndexState(canonical string) (exists, active bool) {
	active, exists = s.indexes.Get(canonical)

	return exists, active
}

// SetIndexActive flips an existing ambiguous index to active on the named
// ledger, for the driver's readiness poller once it has confirmed the index
// READY across replicas. A no-op if the ledger or index is absent (e.g. a
// concurrent DropIndex already removed it). The rebound entry lands in the
// shared ledgers map, updating the committed state; forks copied earlier hold
// their own persistent values and are unaffected. Callers hold the checker
// mutex.
func (g GlobalState) SetIndexActive(ledger, canonical string) {
	g.setIndexReadiness(ledger, canonical, true)
}

// SetIndexAmbiguous flips an existing active index back to ambiguous on the
// named ledger, for the driver's readiness poller when a replica reports the
// index not-ready again (e.g. a restored node rebuilding its read-store). This
// only ever widens what the model tolerates — an ambiguous index accepts both a
// not-ready error and a validated result window — so it can never manufacture a
// finding. Same no-op / rebind semantics as SetIndexActive.
func (g GlobalState) SetIndexAmbiguous(ledger, canonical string) {
	g.setIndexReadiness(ledger, canonical, false)
}

func (g GlobalState) setIndexReadiness(ledger, canonical string, active bool) {
	ls, ok := g.ledgers[ledger]
	if !ok {
		return
	}

	if ls.indexes.Has(canonical) {
		ls.indexes = ls.indexes.Set(canonical, active)
		g.ledgers[ledger] = ls
	}
}

// HasEverAsset reports whether the account has ever touched (base, precision) via
// a committed, non-excluded posting — its membership in the account-by-asset
// index the has-asset filter reads.
func (s LedgerState) HasEverAsset(address, base string, precision uint32) bool {
	return s.everAsset.Has(assetTouch{address: address, base: base, precision: precision})
}

// EverAssetAccounts returns, sorted ascending by address, every account that has
// ever touched (base, precision) — the exact account set a bare has-asset query
// over (base, precision) returns, in the server's address order.
func (s LedgerState) EverAssetAccounts(base string, precision uint32) []string {
	var out []string
	for k := range s.everAsset.All() {
		if k.base == base && k.precision == precision {
			out = append(out, k.address)
		}
	}

	// The map iterates in (address, base, precision) order, so out is sorted.
	return out
}

func (m *metaEffect) Saved() map[string]*commonpb.MetadataValue { return m.saved }

func (r *revertEffect) RevertedID() uint64            { return r.revertedID }
func (r *revertEffect) Postings() []*commonpb.Posting { return r.postings }

// LearnTxStamps fills the server-stamped dates of transaction id that the model
// could not predict at apply time: a nil timestamp, the always-server-stamped
// insertedAt, and a nil revertedAt on a reverted original. Known (client-
// supplied) values are never overwritten — reads validate those directly. The
// values come from the commit response's logs; they are deterministic FSM
// outputs, so folding them in keeps the model exact and lets later reads and
// filter windows check them for equality instead of skipping.
//
// Mutation safety: the record is replaced through the persistent list (whose
// fingerprint the replacement keeps current), and the rebound ledger lands in
// the shared ledgers map, updating the committed state. The caller must hold
// the checker's lock and call this only on the committed state, in the same
// critical section that advanced it — forks copied earlier hold their own
// persistent values and are unaffected.
func (g GlobalState) LearnTxStamps(ledger string, id uint64, timestamp, insertedAt, revertedAt *commonpb.Timestamp) {
	ls, ok := g.ledgers[ledger]
	if !ok || id == 0 || id > uint64(ls.txs.Len()) {
		return
	}

	rec := *ls.txs.Get(int(id - 1))
	if rec.timestamp == nil {
		rec.timestamp = timestamp
	}
	if rec.insertedAt == nil {
		rec.insertedAt = insertedAt
	}
	if rec.revertedAt == nil && rec.reverted {
		rec.revertedAt = revertedAt
	}

	ls.txs = ls.txs.Set(int(id-1), &rec)
	g.ledgers[ledger] = ls
}

// LearnLogDate fills in a log's server-assigned date, the one field of the
// stream the model cannot derive. The id is not learned: it is derived from
// the stream's density, so a server that assigned a different one must be
// caught, not copied.
func (g GlobalState) LearnLogDate(ledger string, id uint64, date *commonpb.Timestamp) {
	ls, ok := g.ledgers[ledger]
	if !ok || id == 0 || id > uint64(ls.logs.Len()) {
		return
	}

	rec := *ls.logs.Get(int(id - 1))
	if rec.date == nil {
		rec.date = date
	}

	ls.logs = ls.logs.Set(int(id-1), &rec)
	g.ledgers[ledger] = ls
}

// LogKinds returns each committed log's kind, ascending by id.
func (s LedgerState) LogKinds() []string {
	out := make([]string, 0, s.logs.Len())
	for i := range s.logs.Len() {
		out = append(out, s.logs.Get(i).kind)
	}

	return out
}

// LogDates returns each committed log's (id, date), ascending. A nil date is
// one not yet learned from a commit response.
func (s LedgerState) LogDates() []struct {
	ID   uint64
	Date *commonpb.Timestamp
} {
	out := make([]struct {
		ID   uint64
		Date *commonpb.Timestamp
	}, 0, s.logs.Len())

	for i := range s.logs.Len() {
		rec := s.logs.Get(i)
		out = append(out, struct {
			ID   uint64
			Date *commonpb.Timestamp
		}{ID: rec.id, Date: rec.date})
	}

	return out
}

// FieldTypesFor returns the declared-type map for a metadata target — the
// schema slice the generator consults for indexable fields.
func (s LedgerState) FieldTypesFor(target commonpb.TargetType) Map[string, commonpb.MetadataType] {
	return s.fieldTypes(target)
}

// FieldTypeFor returns the declared type of one (target, key), false when
// undeclared. Ledger-target keys resolve like the map accessors do.
func (s LedgerState) FieldTypeFor(target commonpb.TargetType, key string) (commonpb.MetadataType, bool) {
	return s.FieldTypesFor(target).Get(key)
}

// RetypeWindow returns the declared types an index's served version may still
// be bound to while retype rewrites converge — the window's accumulated set,
// in enum order — false when no window is open for that canonical index ID.
// The CURRENT declared type is not part of the set; callers always evaluate it
// as the default view.
func (s LedgerState) RetypeWindow(canonical string) ([]commonpb.MetadataType, bool) {
	mask, ok := s.retypeWindows.Get(canonical)
	if !ok {
		return nil, false
	}

	var types []commonpb.MetadataType
	for t := range 32 {
		if mask&(1<<uint(t)) != 0 {
			types = append(types, commonpb.MetadataType(t))
		}
	}

	return types, true
}

// WithDeclaredType returns a view of the state whose declared type for one
// (target, key) is overridden — the evaluation view for one side of a retype
// window. The receiver is untouched; the view shares every other collection.
func (s LedgerState) WithDeclaredType(target commonpb.TargetType, key string, t commonpb.MetadataType) LedgerState {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		s.accountFieldTypes = s.accountFieldTypes.Set(key, t)
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		s.transactionFieldTypes = s.transactionFieldTypes.Set(key, t)
	}

	// The memoized chart derives from types (account types, not metadata
	// types), but keep the view self-contained regardless.
	return s
}

// CloseRetypeWindow removes an index's retype window on the committed state,
// for the driver once it has PROVEN every replica switched: a poll showed the
// retype's own log folded, and a later poll showed no rewrite pending. From
// then on only the new type is legal. Same no-op / rebind semantics as
// SetIndexActive.
func (g GlobalState) CloseRetypeWindow(ledger, canonical string) {
	ls, ok := g.ledgers[ledger]
	if !ok {
		return
	}

	if ls.retypeWindows.Has(canonical) {
		ls.retypeWindows = ls.retypeWindows.Delete(canonical)
		g.ledgers[ledger] = ls
	}
}
