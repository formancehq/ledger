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

type EventLedgerMessage[T any] struct {
	Date    time.Time `json:"date"`
	Type    string    `json:"type"`
	Payload T         `json:"payload"`
	Ledger  string    `json:"ledger"`
}

type CommittedTransactions struct {
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

type SavedMetadata struct {
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

type RevertedTransaction struct {
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

type UpdatedMapping struct {
	Mapping core.Mapping `json:"mapping"`
}
