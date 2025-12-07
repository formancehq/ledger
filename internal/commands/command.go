package commands

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/time"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GenerateRandomID generates a random uint64 ID
func GenerateRandomID() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback to a simple random number if crypto/rand fails
		// This should never happen in practice
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}

// CommandType represents the type of command
type CommandType string

// Command represents a command to be executed in the FSM
type Command struct {
	ID   uint64 // Random command ID
	Type CommandType
	Data []byte    // Binary-encoded command data
	Date time.Time // Creation date in UTC, rounded to microsecond
}

// MarshalBinary encodes the command to binary format using protobuf
func (c *Command) MarshalBinary() ([]byte, error) {
	cmdProto := &CommandProto{
		Id:   c.ID,
		Type: string(c.Type),
		Data: c.Data,
		Date: timestamppb.New(c.Date.Time),
	}
	return proto.Marshal(cmdProto)
}

// UnmarshalBinary decodes the command from binary format using protobuf
func (c *Command) UnmarshalBinary(data []byte) error {
	var cmdProto CommandProto
	if err := proto.Unmarshal(data, &cmdProto); err != nil {
		return err
	}

	c.ID = cmdProto.Id
	c.Type = CommandType(cmdProto.Type)
	c.Data = cmdProto.Data
	if cmdProto.Date != nil {
		c.Date = time.New(cmdProto.Date.AsTime())
	}

	return nil
}

