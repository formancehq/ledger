package bus

import (
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/publish"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/events"
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

func newEventSavedMetadata(savedMetadata SavedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeSavedMetadata,
		Payload: savedMetadata,
	}
}

type RevertedTransaction struct {
	Ledger              string             `json:"ledger"`
	RevertedTransaction ledger.Transaction `json:"revertedTransaction"`
	RevertTransaction   ledger.Transaction `json:"revertTransaction"`
}

func newEventRevertedTransaction(revertedTransaction RevertedTransaction) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeRevertedTransaction,
		Payload: revertedTransaction,
	}
}

type DeletedMetadata struct {
	Ledger     string `json:"ledger"`
	TargetType string `json:"targetType"`
	TargetID   any    `json:"targetId"`
	Key        string `json:"key"`
}

func newEventDeletedMetadata(deletedMetadata DeletedMetadata) publish.EventMessage {
	return publish.EventMessage{
		Date:    time.Now().Time,
		App:     events.EventApp,
		Version: events.EventVersion,
		Type:    events.EventTypeDeletedMetadata,
		Payload: deletedMetadata,
	}
}
