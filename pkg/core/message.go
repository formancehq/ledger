package core

import (
	"encoding/json"
	"fmt"
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

type EventLedgerMessage struct {
	Date    time.Time       `json:"date"`
	App     string          `json:"app"`
	Version string          `json:"version"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	// TODO: deprecated in future version
	Ledger string `json:"ledger"`
}

func (m *EventLedgerMessage) UnmarshalJSON(msg []byte) error {
	fmt.Printf("CUSTOM UNMARSHAL IN: %s\n", string(msg))

	var decoded EventLedgerMessage
	if err := json.Unmarshal(msg, &decoded); err != nil {
		return err
	}

	var dst any
	switch decoded.Type {
	case EventLedgerTypeCommittedTransactions:
		dst = new(CommittedTransactions)
	}
	if err := json.Unmarshal(decoded.Payload, dst); err != nil {
		return err
	}

	fmt.Printf("CUSTOM UNMARSHAL OUT: %+v\n", dst)
	return nil
}

type CommittedTransactions struct {
	Ledger       string                `json:"ledger"`
	Transactions []ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes"`
}

func NewEventLedgerCommittedTransactions(txs CommittedTransactions) EventLedgerMessage {
	payload, err := json.Marshal(txs)
	if err != nil {
		panic(err)
	}

	return EventLedgerMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeCommittedTransactions,
		Payload: payload,
		Ledger:  txs.Ledger,
	}
}

type SavedMetadata struct {
	Ledger     string   `json:"ledger"`
	TargetType string   `json:"targetType"`
	TargetID   string   `json:"targetId"`
	Metadata   Metadata `json:"metadata"`
}

func NewEventLedgerSavedMetadata(metadata SavedMetadata) EventLedgerMessage {
	payload, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}

	return EventLedgerMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeSavedMetadata,
		Payload: payload,
		Ledger:  metadata.Ledger,
	}
}

type UpdatedMapping struct {
	Ledger  string  `json:"ledger"`
	Mapping Mapping `json:"mapping"`
}

func NewEventLedgerUpdatedMapping(mapping UpdatedMapping) EventLedgerMessage {
	payload, err := json.Marshal(mapping)
	if err != nil {
		panic(err)
	}

	return EventLedgerMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeUpdatedMapping,
		Payload: payload,
		Ledger:  mapping.Ledger,
	}
}

type RevertedTransaction struct {
	Ledger              string              `json:"ledger"`
	RevertedTransaction ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   ExpandedTransaction `json:"revertTransaction"`
}

func NewEventLedgerRevertedTransaction(tx RevertedTransaction) EventLedgerMessage {
	payload, err := json.Marshal(tx)
	if err != nil {
		panic(err)
	}

	return EventLedgerMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventLedgerTypeRevertedTransaction,
		Payload: payload,
		Ledger:  tx.Ledger,
	}
}
