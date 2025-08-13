package nats

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// Marshaler provides transport encoding functions
type Marshaler interface {
	// Marshal transforms a watermill message into NATS wire format.
	Marshal(topic string, msg *message.Message) (*nats.Msg, error)
}

// Unmarshaler provides transport decoding function
type Unmarshaler interface {
	// Unmarshal produces a watermill message from NATS wire format.
	Unmarshal(*nats.Msg) (*message.Message, error)
}

// MarshalerUnmarshaler provides both Marshaler and Unmarshaler implementations
type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

func defaultNatsMsg(topic string, data []byte, hdr nats.Header) *nats.Msg {
	return &nats.Msg{
		Subject: topic,
		Data:    data,
		Header:  hdr,
	}
}

// GobMarshaler is marshaller which is using Gob to marshal Watermill messages.
type GobMarshaler struct{}

// Marshal transforms a watermill message into gob format.
func (GobMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	buf := new(bytes.Buffer)

	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return defaultNatsMsg(topic, buf.Bytes(), nil), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (GobMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	buf := new(bytes.Buffer)

	_, err := buf.Write(natsMsg.Data)
	if err != nil {
		return nil, errors.Wrap(err, "cannot write nats message data to buffer")
	}

	decoder := gob.NewDecoder(buf)

	var decodedMsg message.Message
	if err := decoder.Decode(&decodedMsg); err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.UUID, decodedMsg.Payload)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}

// JSONMarshaler uses encoding/json to marshal Watermill messages.
type JSONMarshaler struct{}

// Marshal transforms a watermill message into JSON format.
func (JSONMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return defaultNatsMsg(topic, bytes, nil), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (JSONMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	var decodedMsg message.Message
	err := json.Unmarshal(natsMsg.Data, &decodedMsg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.UUID, decodedMsg.Payload)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}

// NATSMarshaler uses NATS header to marshal directly between watermill and NATS formats.
// The watermill UUID is stored at _watermill_message_uuid
type NATSMarshaler struct{}

// reserved header for NATSMarshaler to send UUID
const WatermillUUIDHdr = "_watermill_message_uuid"

// Marshal transforms a watermill message into JSON format.
func (*NATSMarshaler) Marshal(topic string, msg *message.Message) (*nats.Msg, error) {
	header := make(nats.Header)

	header.Set(WatermillUUIDHdr, msg.UUID)

	for k, v := range msg.Metadata {
		header.Set(k, v)
	}

	data := msg.Payload

	return defaultNatsMsg(topic, data, header), nil
}

// Unmarshal extracts a watermill message from a nats message.
func (*NATSMarshaler) Unmarshal(natsMsg *nats.Msg) (*message.Message, error) {
	data := natsMsg.Data

	hdr := natsMsg.Header

	id := hdr.Get(WatermillUUIDHdr)

	md := make(message.Metadata)

	for k, v := range hdr {
		switch k {
		case WatermillUUIDHdr, nats.MsgIdHdr, nats.ExpectedLastMsgIdHdr, nats.ExpectedStreamHdr, nats.ExpectedLastSubjSeqHdr, nats.ExpectedLastSeqHdr:
			continue
		default:
			if len(v) == 1 {
				md.Set(k, v[0])
			} else {
				return nil, errors.Errorf("multiple values received in NATS header for %q: (%+v)", k, v)
			}
		}
	}

	msg := message.NewMessage(id, data)
	msg.Metadata = md

	return msg, nil
}
