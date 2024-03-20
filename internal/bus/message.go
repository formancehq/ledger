package bus

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/events"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/time"
)

type CommittedTransactions struct {
	Ledger          string                       `json:"ledger"`
	Transactions    []ledger.Transaction         `json:"transactions"`
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata"`
}

func newEventCommittedTransactions(txs CommittedTransactions) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
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

func newEventSavedMetadata(metadata SavedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
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

func newEventRevertedTransaction(tx RevertedTransaction) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeRevertedTransaction,
		Payload: tx,
	}
}

type DeletedMetadata struct {
	Ledger     string `json:"ledger"`
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func newEventDeletedMetadata(tx DeletedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeDeletedMetadata,
		Payload: tx,
	}
}
