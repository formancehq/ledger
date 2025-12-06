package bucketfsm

import (
	"encoding/json"

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
	data, err := json.Marshal(CreateLedgerCommand{
		Name:     name,
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateLedger,
		Data: data,
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
	data, err := json.Marshal(CreateTransactionCommand{
		LedgerName:        ledgerName,
		CreateTransaction: createTx,
		IdempotencyKey:    idempotencyKey,
		DryRun:            dryRun,
	})
	if err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateTransaction,
		Data: data,
		Date: time.Now(),
	}, nil
}
