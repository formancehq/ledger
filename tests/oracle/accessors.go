package oracle

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// Exported read accessors over the model's internal state. The driver and the
// replay tool inspect a committed state (chart, volumes, metadata, declared
// field types) to generate operations and compare against the SUT; the maps are
// never mutated through these — every model mutation goes through Apply.

func (g GlobalState) Ledgers() map[string]LedgerState { return g.ledgers }

func (s LedgerState) Types() map[string]TypeState                         { return s.types }
func (s LedgerState) Volumes() map[VolumeKey]VolumePair                   { return s.volumes }
func (s LedgerState) Metadata() map[MetaKey]*commonpb.MetadataValue       { return s.metadata }
func (s LedgerState) LedgerMeta() map[string]*commonpb.MetadataValue      { return s.ledgerMeta }
func (s LedgerState) AccountFieldTypes() map[string]commonpb.MetadataType { return s.accountFieldTypes }
func (s LedgerState) LedgerFieldTypes() map[string]commonpb.MetadataType  { return s.ledgerFieldTypes }
func (s LedgerState) TransactionFieldTypes() map[string]commonpb.MetadataType {
	return s.transactionFieldTypes
}

// Txs is the transaction log; index i holds the transaction with id i+1. TxByRef
// indexes referenced transactions by reference -> id.
func (s LedgerState) Txs() []*txRecord        { return s.txs }
func (s LedgerState) TxByRef() map[string]int { return s.txByRef }

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
