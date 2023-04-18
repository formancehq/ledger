package publish

import (
	"context"
	"encoding/json"

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
