package events

import (
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/publish"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
)

type CommittedTransactions struct {
	Ledger          string                       `json:"ledger"`
	Transactions    []ledger.Transaction         `json:"transactions"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata"`
}

func NewEventCommittedTransactions(txs CommittedTransactions) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeCommittedTransactions,
		Payload: txs,
	}
}

type SavedMetadata struct {
	Ledger     string            `json:"ledger"`
	TargetType string            `json:"targetType"`
	TargetID   string            `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

func NewEventSavedMetadata(savedMetadata SavedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedMetadata,
		Payload: savedMetadata,
	}
}

type RevertedTransaction struct {
	Ledger              string             `json:"ledger"`
	RevertedTransaction ledger.Transaction `json:"revertedTransaction"`
	RevertTransaction   ledger.Transaction `json:"revertTransaction"`
}

func NewEventRevertedTransaction(revertedTransaction RevertedTransaction) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeRevertedTransaction,
		Payload: revertedTransaction,
	}
}

type DeletedMetadata struct {
	Ledger     string `json:"ledger"`
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func NewEventDeletedMetadata(deletedMetadata DeletedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeDeletedMetadata,
		Payload: deletedMetadata,
	}
}

type UpdatedSchema struct {
	Ledger string        `json:"ledger"`
	Schema ledger.Schema `json:"schema"`
}

func NewEventUpdatedSchema(updatedSchema UpdatedSchema) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeUpdatedSchema,
		Payload: updatedSchema,
	}
}
