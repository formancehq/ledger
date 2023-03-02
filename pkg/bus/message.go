package bus

import (
	"time"

	"github.com/formancehq/ledger/pkg/core"
)

const (
	EventVersion = "v1"
	EventApp     = "ledger"

	EventTypeCommittedTransactions = "COMMITTED_TRANSACTIONS"
	EventTypeSavedMetadata         = "SAVED_METADATA"
	EventTypeUpdatedMapping        = "UPDATED_MAPPING"
	EventTypeRevertedTransaction   = "REVERTED_TRANSACTION"
)

type EventMessage struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload any       `json:"payload"`
}

type CommittedTransactions struct {
	Ledger       string                     `json:"ledger"`
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func newEventCommittedTransactions(txs CommittedTransactions) EventMessage {
	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeCommittedTransactions,
		Payload: txs,
	}
}

type SavedMetadata struct {
	Ledger     string        `json:"ledger"`
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

func newEventSavedMetadata(metadata SavedMetadata) EventMessage {
	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedMetadata,
		Payload: metadata,
	}
}

type UpdatedMapping struct {
	Ledger  string       `json:"ledger"`
	Mapping core.Mapping `json:"mapping"`
}

func newEventUpdatedMapping(mapping UpdatedMapping) EventMessage {
	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeUpdatedMapping,
		Payload: mapping,
	}
}

type RevertedTransaction struct {
	Ledger              string                   `json:"ledger"`
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

func newEventRevertedTransaction(tx RevertedTransaction) EventMessage {
	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeRevertedTransaction,
		Payload: tx,
	}
}
