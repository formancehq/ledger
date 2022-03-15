package bus

import (
	"context"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pborman/uuid"
	"time"
)

func newMessage(ctx context.Context, m interface{}) *message.Message {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return message.NewMessage(uuid.New(), data)
}

type baseEvent struct {
	Date    time.Time   `json:"date"`
	Type    eventType   `json:"type"`
	Payload interface{} `json:"payload"`
	Ledger  string      `json:"ledger"`
}

type committedTransactions struct {
	Transactions []ledger.CommitTransactionResult `json:"transactions"`
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
