package service

import (
	"encoding/json"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// CommandType represents the type of command
type CommandType string

const (
	// CommandTypeInsertLogs is the command type for inserting ledger logs
	CommandTypeInsertLogs CommandType = "insert_logs"
	// CommandTypeSetPublicAddr is the command type for setting a node's public address
	CommandTypeSetPublicAddr CommandType = "set_public_addr"
	// CommandTypeCreateLedger is the command type for creating a new ledger
	CommandTypeCreateLedger CommandType = "create_ledger"
	// CommandTypeCreateBucket is the command type for creating a new bucket
	CommandTypeCreateBucket CommandType = "create_bucket"
	// CommandTypeDeleteBucket is the command type for deleting a bucket
	CommandTypeDeleteBucket CommandType = "delete_bucket"
)

// Command represents a command to be executed in the FSM
type Command struct {
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

// InsertLogsCommand represents the data for an insert logs command
type InsertLogsCommand struct {
	Logs []ledger.Log `json:"logs"`
}

// NewInsertLogsCommand creates a new InsertLogsCommand
func NewInsertLogsCommand(logs []ledger.Log) (*Command, error) {
	data, err := json.Marshal(InsertLogsCommand{Logs: logs})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeInsertLogs,
		Data: data,
	}, nil
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
		Type: CommandTypeSetPublicAddr,
		Data: data,
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

// CreateLedgerCommand represents the data for a create ledger command
type CreateLedgerCommand struct {
	Name     string            `json:"name"`               // Ledger name/ID (required)
	Metadata metadata.Metadata `json:"metadata,omitempty"` // Optional metadata
}

// NewCreateLedgerCommand creates a new CreateLedgerCommand
func NewCreateLedgerCommand(name string, metadata metadata.Metadata) (*Command, error) {
	data, err := json.Marshal(CreateLedgerCommand{
		Name:     name,
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeCreateLedger,
		Data: data,
	}, nil
}

// BucketInfo represents information about a bucket
type BucketInfo struct {
	ID        uint64                 `json:"id"`        // Sequential bucket ID
	Name      string                 `json:"name"`      // Bucket name/ID
	Driver    string                 `json:"driver"`    // Driver name (e.g., "postgres", "s3", etc.)
	Config    map[string]interface{} `json:"config"`    // Driver-specific configuration
	CreatedAt time.Time              `json:"createdAt"` // Creation timestamp
}

// CreateBucketCommand represents the data for a create bucket command
type CreateBucketCommand struct {
	Name   string                 `json:"name"`   // Bucket name/ID (required)
	Driver string                 `json:"driver"` // Driver name (required)
	Config map[string]interface{} `json:"config"` // Driver-specific configuration (required)
}

// NewCreateBucketCommand creates a new CreateBucketCommand
func NewCreateBucketCommand(name, driver string, config map[string]interface{}) (*Command, error) {
	data, err := json.Marshal(CreateBucketCommand{
		Name:   name,
		Driver: driver,
		Config: config,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeCreateBucket,
		Data: data,
	}, nil
}

// DeleteBucketCommand represents the data for a delete bucket command
type DeleteBucketCommand struct {
	Name string `json:"name"` // Bucket name/ID (required)
}

// NewDeleteBucketCommand creates a new DeleteBucketCommand
func NewDeleteBucketCommand(name string) (*Command, error) {
	data, err := json.Marshal(DeleteBucketCommand{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type: CommandTypeDeleteBucket,
		Data: data,
	}, nil
}
