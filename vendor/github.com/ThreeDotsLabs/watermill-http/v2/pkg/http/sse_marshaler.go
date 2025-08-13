package http

import (
	"context"
	"encoding/json"
	"fmt"
)

type ServerSentEvent struct {
	Event string
	Data  []byte
}

type SSEMarshaler interface {
	Marshal(ctx context.Context, payload any) (ServerSentEvent, error)
}

type JSONSSEMarshaler struct{}

func (j JSONSSEMarshaler) Marshal(ctx context.Context, payload any) (ServerSentEvent, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return ServerSentEvent{}, err
	}

	return ServerSentEvent{
		Event: "data",
		Data:  data,
	}, nil
}

type StringSSEMarshaler struct{}

func (s StringSSEMarshaler) Marshal(ctx context.Context, payload any) (ServerSentEvent, error) {
	data := fmt.Sprint(payload)

	return ServerSentEvent{
		Event: "data",
		Data:  []byte(data),
	}, nil
}
