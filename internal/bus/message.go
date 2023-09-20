package bus

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/events"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type EventMessage struct {
	Date    ledger.Time `json:"date"`
	App     string      `json:"app"`
	Version string      `json:"version"`
	Type    string      `json:"type"`
	Payload any         `json:"payload"`
}

type CommittedTransactions struct {
	Ledger          string                       `json:"ledger"`
	Transaction     ledger.Transaction           `json:"transaction"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata"`
}

func newEventCommittedTransactions(txs CommittedTransactions) EventMessage {
	return EventMessage{
		Date:    ledger.Now(),
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeCommittedTransactions,
		Payload: txs,
	}
}

type SavedMetadata struct {
	Ledger     string            `json:"ledger"`
	TargetType string            `json:"targetType"`
	TargetID   string            `json:"targetId"`
	Metadata   metadata.Metadata `json:"metadata"`
}

func newEventSavedMetadata(metadata SavedMetadata) EventMessage {
	return EventMessage{
		Date:    ledger.Now(),
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeSavedMetadata,
		Payload: metadata,
	}
}

type RevertedTransaction struct {
	Ledger              string             `json:"ledger"`
	RevertedTransaction ledger.Transaction `json:"revertedTransaction"`
	RevertTransaction   ledger.Transaction `json:"revertTransaction"`
}

func newEventRevertedTransaction(tx RevertedTransaction) EventMessage {
	return EventMessage{
		Date:    ledger.Now(),
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeRevertedTransaction,
		Payload: tx,
	}
}

type DeletedMetadata struct {
	Ledger     string `json:"ledger"`
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetID"`
	Key        string `json:"key"`
}

func newEventDeletedMetadata(tx DeletedMetadata) EventMessage {
	return EventMessage{
		Date:    ledger.Now(),
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeDeletedMetadata,
		Payload: tx,
	}
}
