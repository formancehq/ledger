package service

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/metadata"
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

// LedgerInfo represents information about a ledger
type LedgerInfo struct {
	ID        uint64            `json:"id"`        // Sequential ID for the ledger
	Name      string            `json:"name"`      // Ledger name/ID
	CreatedAt time.Time         `json:"createdAt"` // Creation timestamp
	Metadata  metadata.Metadata `json:"metadata,omitempty"`
	LastLogID *uint64           `json:"lastLogId,omitempty"` // ID of the last log for this ledger
}

// BucketInfo represents information about a bucket
type BucketInfo struct {
	ID        uint64                 `json:"id"`        // Sequential bucket ID
	Name      string                 `json:"name"`      // Bucket name/ID
	Driver    string                 `json:"driver"`    // Driver name (e.g., "postgres", "s3", etc.)
	Config    map[string]interface{} `json:"config"`    // Driver-specific configuration
	CreatedAt time.Time              `json:"createdAt"` // Creation timestamp
}
