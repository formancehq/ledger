package bus

import (
	"time"

	"github.com/numary/ledger/pkg/core"
)

type baseEvent struct {
	Date    time.Time   `json:"date"`
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
	Ledger  string      `json:"ledger"`
}

type committedTransactions struct {
	Transactions []core.Transaction     `json:"transactions"`
	Volumes      core.AggregatedVolumes `json:"volumes"`
}

type savedMetadata struct {
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

type revertedTransaction struct {
	RevertedTransaction core.Transaction `json:"revertedTransaction"`
	RevertTransaction   core.Transaction `json:"revertTransaction"`
}

type updatedMapping struct {
	Mapping core.Mapping `json:"mapping"`
}
