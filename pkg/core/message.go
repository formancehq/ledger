package core

import (
	"time"
)

const (
	EventVersion = "v1"
	EventApp     = "ledger"

	EventLedgerTypeCommittedTransactions = "COMMITTED_TRANSACTIONS"
	EventLedgerTypeSavedMetadata         = "SAVED_METADATA"
	EventLedgerTypeUpdatedMapping        = "UPDATED_MAPPING"
	EventLedgerTypeRevertedTransaction   = "REVERTED_TRANSACTION"
)

type EventLedgerMessage[T any] struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload T         `json:"payload"`
	// TODO: deprecated in future version
	Ledger string `json:"ledger"`
}

type CommittedTransactions struct {
	Ledger       string                `json:"ledger"`
	Transactions []ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func NewEventLedgerCommittedTransactions(payload CommittedTransactions) EventLedgerMessage[CommittedTransactions] {
	return EventLedgerMessage[CommittedTransactions]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeCommittedTransactions,
		Payload: payload,
		Ledger:  payload.Ledger,
	}
}

type SavedMetadata struct {
	Ledger     string   `json:"ledger"`
	TargetType string   `json:"targetType"`
	TargetID   string   `json:"targetId"`
	Metadata   Metadata `json:"metadata"`
}

func NewEventLedgerSavedMetadata(payload SavedMetadata) EventLedgerMessage[SavedMetadata] {
	return EventLedgerMessage[SavedMetadata]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeSavedMetadata,
		Payload: payload,
		Ledger:  payload.Ledger,
	}
}

type UpdatedMapping struct {
	Ledger  string  `json:"ledger"`
	Mapping Mapping `json:"mapping"`
}

func NewEventLedgerUpdatedMapping(payload UpdatedMapping) EventLedgerMessage[UpdatedMapping] {
	return EventLedgerMessage[UpdatedMapping]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeUpdatedMapping,
		Payload: payload,
		Ledger:  payload.Ledger,
	}
}

type RevertedTransaction struct {
	Ledger              string              `json:"ledger"`
	RevertedTransaction ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   ExpandedTransaction `json:"revertTransaction"`
}

func NewEventLedgerRevertedTransaction(payload RevertedTransaction) EventLedgerMessage[RevertedTransaction] {
	return EventLedgerMessage[RevertedTransaction]{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeRevertedTransaction,
		Payload: payload,
		Ledger:  payload.Ledger,
	}
}
