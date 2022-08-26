package core

import (
	"errors"
	"time"
)

const (
	EventLedgerCommittedTransactions = "COMMITTED_TRANSACTIONS"
	EventLedgerSavedMetadata         = "SAVED_METADATA"
	EventLedgerUpdatedMapping        = "UPDATED_MAPPING"
	EventLedgerRevertedTransaction   = "REVERTED_TRANSACTION"
)

type EventLedgerMessage[T any] struct {
	Date    time.Time `json:"date"`
	Type    string    `json:"type"`
	Payload T         `json:"payload"`
	Ledger  string    `json:"ledger"`
}

var (
	ErrMessageDateZero    = errors.New("message date cannot be zero")
	ErrMessageTypeEmpty   = errors.New("message type cannot be empty")
	ErrMessageLedgerEmpty = errors.New("message ledger cannot be empty")
)

func (e *EventLedgerMessage[T]) Validate() error {
	if e.Date.IsZero() {
		return ErrMessageDateZero
	}

	if e.Type == "" {
		return ErrMessageTypeEmpty
	}

	if e.Ledger == "" {
		return ErrMessageLedgerEmpty
	}

	return nil
}

type CommittedTransactions struct {
	Transactions []ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes"`
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
	TargetType string   `json:"targetType"`
	TargetID   string   `json:"targetId"`
	Metadata   Metadata `json:"metadata"`
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
	Mapping Mapping `json:"mapping"`
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
	RevertedTransaction ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   ExpandedTransaction `json:"revertTransaction"`
}

func NewEventLedgerRevertedTransaction(payload RevertedTransaction, ledger string) EventLedgerMessage[RevertedTransaction] {
	return EventLedgerMessage[RevertedTransaction]{
		Date:    time.Now().UTC(),
		Type:    EventLedgerRevertedTransaction,
		Payload: payload,
		Ledger:  ledger,
	}
}
