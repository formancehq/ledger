package bucketfsm

import (
	"bytes"
	"encoding/gob"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

func init() {
	// Register all types that can be stored in ledger.Log.Data (LogPayload interface)
	// This is required for gob encoding/decoding to work with interface types
	gob.Register(&ledger.CreatedTransaction{})
	gob.Register(&ledger.RevertedTransaction{})
	gob.Register(&ledger.SavedMetadata{})
	gob.Register(&ledger.DeletedMetadata{})
	
	// Register command types
	gob.Register(CreateLedgerCommand{})
	gob.Register(InsertLogCommand{})
}

const (
	// CommandTypeCreateLedger is the command type for creating a new ledger
	CommandTypeCreateLedger service.CommandType = "create_ledger"
	// CommandTypeInsertLog is the command type for inserting a log
	CommandTypeInsertLog service.CommandType = "insert_log"
)

// CreateLedgerCommand represents the data for a create ledger command
type CreateLedgerCommand struct {
	Name     string            `json:"name"`               // Ledger name/ID (required)
	Metadata metadata.Metadata `json:"metadata,omitempty"` // Optional metadata
}

// NewCreateLedgerCommand creates a new CreateLedgerCommand
func NewCreateLedgerCommand(name string, metadata metadata.Metadata) (*service.Command, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(CreateLedgerCommand{
		Name:     name,
		Metadata: metadata,
	}); err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateLedger,
		Data: buf.Bytes(),
		Date: time.Now(),
	}, nil
}

// InsertLogCommand represents the data for an insert log command
type InsertLogCommand struct {
	Log ledger.Log `json:"log"` // Log to insert
}

// NewInsertLogCommand creates a new InsertLogCommand
func NewInsertLogCommand(log ledger.Log) (*service.Command, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(InsertLogCommand{
		Log: log,
	}); err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeInsertLog,
		Data: buf.Bytes(),
		Date: time.Now(),
	}, nil
}

// UnmarshalCommandData unmarshals command data from binary format
func UnmarshalCommandData(data []byte, v interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}
