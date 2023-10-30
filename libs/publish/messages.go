package publish

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

func NewMessage(ctx context.Context, m any) *message.Message {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	msg := message.NewMessage(uuid.NewString(), data)
	msg.SetContext(ctx)
	return msg
}

type EventMessage struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload any       `json:"payload"`
}
