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
	Transactions []core.ExpandedTransaction `json:"transactions"`
	// Deprecated (use postCommitVolumes)
	Volumes           core.AccountsAssetsVolumes `json:"volumes"`
	PostCommitVolumes core.AccountsAssetsVolumes `json:"postCommitVolumes"`
	PreCommitVolumes  core.AccountsAssetsVolumes `json:"preCommitVolumes"`
}

type savedMetadata struct {
	TargetType string        `json:"targetType"`
	TargetID   string        `json:"targetId"`
	Metadata   core.Metadata `json:"metadata"`
}

type revertedTransaction struct {
	RevertedTransaction core.ExpandedTransaction `json:"revertedTransaction"`
	RevertTransaction   core.ExpandedTransaction `json:"revertTransaction"`
}

type updatedMapping struct {
	Mapping core.Mapping `json:"mapping"`
}
