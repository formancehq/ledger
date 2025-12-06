package bucketfsm

import (
	"bytes"
	"encoding/gob"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

const (
	// CommandTypeCreateLedger is the command type for creating a new ledger
	CommandTypeCreateLedger service.CommandType = "create_ledger"
	// CommandTypeCreateTransaction is the command type for creating a transaction
	CommandTypeCreateTransaction service.CommandType = "create_transaction"
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

// CreateTransactionCommand represents the data for a create transaction command
type CreateTransactionCommand struct {
	LedgerName        string                    `json:"ledgerName"`               // Ledger name (required)
	CreateTransaction service.CreateTransaction `json:"createTransaction"`        // Transaction creation parameters
	IdempotencyKey    string                    `json:"idempotencyKey,omitempty"` // Optional idempotency key
	DryRun            bool                      `json:"dryRun"`                   // Whether this is a dry run
}

// NewCreateTransactionCommand creates a new CreateTransactionCommand
func NewCreateTransactionCommand(ledgerName string, createTx service.CreateTransaction, idempotencyKey string, dryRun bool) (*service.Command, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(CreateTransactionCommand{
		LedgerName:        ledgerName,
		CreateTransaction: createTx,
		IdempotencyKey:    idempotencyKey,
		DryRun:            dryRun,
	}); err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateTransaction,
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
