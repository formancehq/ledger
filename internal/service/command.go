package service

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
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

const (
	// CommandTypeSetPublicAddr is the command type for setting a node's public address
	CommandTypeSetPublicAddr CommandType = "set_public_addr"
)

// Command represents a command to be executed in the FSM
type Command struct {
	ID   uint64 // Random command ID
	Type CommandType
	Data []byte    // Binary-encoded command data
	Date time.Time // Creation date in UTC, rounded to microsecond
}

// MarshalBinary encodes the command to binary format
func (c *Command) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Encode command header
	if err := enc.Encode(c.ID); err != nil {
		return nil, err
	}
	if err := enc.Encode(c.Type); err != nil {
		return nil, err
	}
	if err := enc.Encode(c.Date); err != nil {
		return nil, err
	}

	// Encode data length and data
	if err := enc.Encode(uint32(len(c.Data))); err != nil {
		return nil, err
	}
	if len(c.Data) > 0 {
		if err := enc.Encode(c.Data); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary decodes the command from binary format
func (c *Command) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	// Decode command header
	if err := dec.Decode(&c.ID); err != nil {
		return err
	}
	if err := dec.Decode(&c.Type); err != nil {
		return err
	}
	if err := dec.Decode(&c.Date); err != nil {
		return err
	}

	// Decode data length and data
	var dataLen uint32
	if err := dec.Decode(&dataLen); err != nil {
		return err
	}
	if dataLen > 0 {
		c.Data = make([]byte, dataLen)
		if err := dec.Decode(&c.Data); err != nil {
			return err
		}
	} else {
		c.Data = nil
	}

	return nil
}

// SetPublicAddrCommand represents the data for a set public address command
type SetPublicAddrCommand struct {
	NodeID     string `json:"nodeId"`
	PublicAddr string `json:"publicAddr"`
}

// NewSetPublicAddrCommand creates a new SetPublicAddrCommand
func NewSetPublicAddrCommand(nodeID, publicAddr string) (*Command, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(SetPublicAddrCommand{
		NodeID:     nodeID,
		PublicAddr: publicAddr,
	}); err != nil {
		return nil, err
	}
	return &Command{
		ID:   GenerateRandomID(),
		Type: CommandTypeSetPublicAddr,
		Data: buf.Bytes(),
		Date: time.Now(),
	}, nil
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
