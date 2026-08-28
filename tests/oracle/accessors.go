package oracle

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// Exported read accessors over the model's internal state. The driver and the
// replay tool inspect a committed state (chart, volumes, metadata, declared
// field types) to generate operations and compare against the SUT. The
// collections are persistent (see pmap.go), so handing them out cannot expose
// the model to mutation — every model mutation goes through Apply. Their
// iteration order is sorted, so callers may draw Antithesis-reproducible
// decisions while ranging.

func (g GlobalState) Ledgers() map[string]LedgerState { return g.ledgers }

// Chapters is the bucket-global chapter registry.
func (g GlobalState) Chapters() Chapters { return g.chapters }

// WithChapters replaces the chapter registry. The driver pins the model to a
// registry it observed with it, and hypothesises a pending autonomous transition
// (WithSealed/WithConfirmed) the same way — the two the server makes on its own,
// which no order announces.
func (g GlobalState) WithChapters(c Chapters) GlobalState {
	g.chapters = c

	return g
}

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
func (t *txRecord) RevertedBy() uint64                           { return t.revertedBy }
func (t *txRecord) RevertedAt() *commonpb.Timestamp              { return t.revertedAt }
func (t *txRecord) RevertsTransaction() uint64                   { return t.revertsTransaction }

func (m *metaEffect) Saved() map[string]*commonpb.MetadataValue { return m.saved }

func (r *revertEffect) RevertedID() uint64            { return r.revertedID }
func (r *revertEffect) Postings() []*commonpb.Posting { return r.postings }
