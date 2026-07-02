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
func (s LedgerState) TxRefs() map[string]*txRecord                        { return s.txRefs }
func (s LedgerState) TxMeta() map[TxMetaKey]*commonpb.MetadataValue       { return s.txMeta }
func (s LedgerState) TransactionFieldTypes() map[string]commonpb.MetadataType {
	return s.transactionFieldTypes
}

// txRecord accessors expose a committed transaction tracked by reference: its
// server-assigned id (the generator resolves a reference to its id to target
// it), its original postings, and whether it has been reverted.
func (t *txRecord) Id() uint64                    { return t.id }
func (t *txRecord) Postings() []*commonpb.Posting { return t.postings }
func (t *txRecord) Reverted() bool                { return t.reverted }

func (m *metaEffect) Saved() map[string]*commonpb.MetadataValue { return m.saved }

func (r *revertEffect) RevertedID() uint64            { return r.revertedID }
func (r *revertEffect) Postings() []*commonpb.Posting { return r.postings }

// Saved is the metadata set on the revert transaction (empty when the revert
// carried none), echoed verbatim on the RevertedTransaction log.
func (r *revertEffect) Saved() map[string]*commonpb.MetadataValue { return r.saved }
