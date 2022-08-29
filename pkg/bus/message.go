package bus

import (
	"encoding/json"
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

type EventMessage struct {
	Date    time.Time       `json:"date"`
	App     string          `json:"app"`
	Version string          `json:"version"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	// TODO: deprecated in future version
	Ledger string `json:"ledger"`
}

type CommittedTransactions struct {
	Ledger       string                     `json:"ledger"`
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func NewEventCommittedTransactions(txs CommittedTransactions) EventMessage {
	payload, err := json.Marshal(txs)
	if err != nil {
		panic(err)
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeCommittedTransactions,
		Payload: payload,
		Ledger:  txs.Ledger,
	}
}

type SavedMetadata struct {
	Ledger     string        `json:"ledger"`
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

func NewEventSavedMetadata(metadata SavedMetadata) EventMessage {
	payload, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedMetadata,
		Payload: payload,
		Ledger:  metadata.Ledger,
	}
}

type UpdatedMapping struct {
	Ledger  string       `json:"ledger"`
	Mapping core.Mapping `json:"mapping"`
}

func NewEventUpdatedMapping(mapping UpdatedMapping) EventMessage {
	payload, err := json.Marshal(mapping)
	if err != nil {
		panic(err)
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeUpdatedMapping,
		Payload: payload,
		Ledger:  mapping.Ledger,
	}
}

type RevertedTransaction struct {
	Ledger              string                   `json:"ledger"`
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

func NewEventRevertedTransaction(tx RevertedTransaction) EventMessage {
	payload, err := json.Marshal(tx)
	if err != nil {
		panic(err)
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeRevertedTransaction,
		Payload: payload,
		Ledger:  tx.Ledger,
	}
}
