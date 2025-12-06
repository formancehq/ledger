package service

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"

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
	ID   uint64          `json:"id"` // Random command ID
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
	Date time.Time       `json:"date"` // Creation date in UTC, rounded to microsecond
}

// SetPublicAddrCommand represents the data for a set public address command
type SetPublicAddrCommand struct {
	NodeID     string `json:"nodeId"`
	PublicAddr string `json:"publicAddr"`
}

// NewSetPublicAddrCommand creates a new SetPublicAddrCommand
func NewSetPublicAddrCommand(nodeID, publicAddr string) (*Command, error) {
	data, err := json.Marshal(SetPublicAddrCommand{
		NodeID:     nodeID,
		PublicAddr: publicAddr,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		ID:   GenerateRandomID(),
		Type: CommandTypeSetPublicAddr,
		Data: data,
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
