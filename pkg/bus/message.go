package bus

import (
	"time"

	"github.com/numary/ledger/pkg/core"
)

const (
	EventLedgerCommittedTransactions = "COMMITTED_TRANSACTIONS"
	EventLedgerSavedMetadata         = "SAVED_METADATA"
	EventLedgerUpdatedMapping        = "UPDATED_MAPPING"
	EventLedgerRevertedTransaction   = "REVERTED_TRANSACTION"
)

type EventLedgerMessage[P any] struct {
	Date    time.Time `json:"date"`
	Type    string    `json:"type"`
	Payload P         `json:"payload"`
	Ledger  string    `json:"ledger"`
}

type CommittedTransactions struct {
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func NewEventLedgerCommittedTransactions(payload CommittedTransactions, ledger string) EventLedgerMessage[CommittedTransactions] {
	return EventLedgerMessage[CommittedTransactions]{
		Date:    time.Now().UTC(),
		Type:    EventLedgerCommittedTransactions,
		Payload: payload,
		Ledger:  ledger,
	}
}

type SavedMetadata struct {
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

func NewEventLedgerSavedMetadata(payload SavedMetadata, ledger string) EventLedgerMessage[SavedMetadata] {
	return EventLedgerMessage[SavedMetadata]{
		Date:    time.Now().UTC(),
		Type:    EventLedgerSavedMetadata,
		Payload: payload,
		Ledger:  ledger,
	}
}

type UpdatedMapping struct {
	Mapping core.Mapping `json:"mapping"`
}

func NewEventLedgerUpdatedMapping(payload UpdatedMapping, ledger string) EventLedgerMessage[UpdatedMapping] {
	return EventLedgerMessage[UpdatedMapping]{
		Date:    time.Now().UTC(),
		Type:    EventLedgerUpdatedMapping,
		Payload: payload,
		Ledger:  ledger,
	}
}

type RevertedTransaction struct {
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

func NewEventLedgerRevertedTransaction(payload RevertedTransaction, ledger string) EventLedgerMessage[RevertedTransaction] {
	return EventLedgerMessage[RevertedTransaction]{
		Date:    time.Now().UTC(),
		Type:    EventLedgerRevertedTransaction,
		Payload: payload,
		Ledger:  ledger,
	}
}
