package bus

import (
	"time"

	"github.com/numary/ledger/pkg/core"
)

const (
	EventVersion = "v1"
	EventApp     = "ledger"

	EventTypeCommittedTransactions = "COMMITTED_TRANSACTIONS"
	EventTypeSavedMetadata         = "SAVED_METADATA"
	EventTypeUpdatedMapping        = "UPDATED_MAPPING"
	EventTypeRevertedTransaction   = "REVERTED_TRANSACTION"
)

type EventMessage[PAYLOAD Payload] struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload PAYLOAD   `json:"payload"`
	// TODO: deprecated in future version
	Ledger string `json:"ledger"`
}

type Payload interface {
	PayloadType() string
}

type CommittedTransactions struct {
	Ledger       string                     `json:"ledger"`
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func (c CommittedTransactions) PayloadType() string {
	return EventTypeCommittedTransactions
}

func newEventCommittedTransactions(txs CommittedTransactions) EventMessage[CommittedTransactions] {
	return EventMessage[CommittedTransactions]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeCommittedTransactions,
		Payload: txs,
		Ledger:  txs.Ledger,
	}
}

type SavedMetadata struct {
	Ledger     string        `json:"ledger"`
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

func (s SavedMetadata) PayloadType() string {
	return EventTypeSavedMetadata
}

func newEventSavedMetadata(metadata SavedMetadata) EventMessage[SavedMetadata] {
	return EventMessage[SavedMetadata]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedMetadata,
		Payload: metadata,
		Ledger:  metadata.Ledger,
	}
}

type UpdatedMapping struct {
	Ledger  string       `json:"ledger"`
	Mapping core.Mapping `json:"mapping"`
}

func (u UpdatedMapping) PayloadType() string {
	return EventTypeUpdatedMapping
}

func newEventUpdatedMapping(mapping UpdatedMapping) EventMessage[UpdatedMapping] {
	return EventMessage[UpdatedMapping]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeUpdatedMapping,
		Payload: mapping,
		Ledger:  mapping.Ledger,
	}
}

type RevertedTransaction struct {
	Ledger              string                   `json:"ledger"`
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

func (r RevertedTransaction) PayloadType() string {
	return EventTypeRevertedTransaction
}

func newEventRevertedTransaction(tx RevertedTransaction) EventMessage[RevertedTransaction] {
	return EventMessage[RevertedTransaction]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeRevertedTransaction,
		Payload: tx,
		Ledger:  tx.Ledger,
	}
}
