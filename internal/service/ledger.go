package service

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type Ledger interface {
	// CreateTransaction accept a numscript script and returns a transaction
	// It can return following errors:
	//  * ErrCompilationFailed
	//  * ErrMetadataOverride
	//  * ErrInvalidVars
	//  * ErrTransactionReferenceConflict
	//  * ErrIdempotencyKeyConflict
	//  * ErrInsufficientFunds
	CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error)
	// RevertTransaction allow to revert a transaction.
	// It can return following errors:
	//  * ErrInsufficientFunds
	//  * ErrAlreadyReverted
	//  * ErrNotFound
	// Parameter force indicate we want to force revert the transaction even if the accounts does not have funds
	// Parameter atEffectiveDate indicate we want to set the timestamp of the newly created transaction on the timestamp of the reverted transaction
	RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error)
	// SaveTransactionMetadata allow to add metadata to an existing transaction
	// It can return following errors:
	//  * ErrNotFound
	SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error)
	// SaveAccountMetadata allow to add metadata to an account
	// If the account does not exist, it is created
	SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error)
	// DeleteTransactionMetadata allow to remove metadata of a transaction
	// It can return following errors:
	//  * ErrNotFound : indicate the transaction was not found OR the metadata does not exist on the transaction
	DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error)
	// DeleteAccountMetadata allow to remove metadata of an account
	// It can return following errors:
	//  * ErrNotFound : indicate the account was not found OR the metadata does not exist on the account
	DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error)
	// Import allow to import the logs of an existing ledger
	// It can return following errors:
	//  * ErrImport
	// Logs hash must be valid and the ledger.Ledger must be in 'initializing' state
	Import(ctx context.Context, stream chan ledger.Log) error
	// Export allow to export the logs of a ledger
	Export(ctx context.Context, w ExportWriter) error
}

type Parameters[INPUT any] struct {
	DryRun         bool
	IdempotencyKey string
	Input          INPUT
}

type CreateTransaction struct {
	AccountMetadata map[string]metadata.Metadata
	Timestamp time.Time         `json:"timestamp"`
	Metadata  metadata.Metadata `json:"metadata"`
	Reference string            `json:"reference"`
	Postings ledger.Postings
}

type RevertTransaction struct {
	Force           bool
	AtEffectiveDate bool
	TransactionID   uint64
	Metadata        metadata.Metadata
}

type SaveTransactionMetadata struct {
	TransactionID uint64
	Metadata      metadata.Metadata
}

type SaveAccountMetadata struct {
	Address  string
	Metadata metadata.Metadata
}

type DeleteTransactionMetadata struct {
	TransactionID uint64
	Key           string
}

type DeleteAccountMetadata struct {
	Address string
	Key     string
}

type ExportWriter interface {
	Write(ctx context.Context, log ledger.Log) error
}

type ExportWriterFn func(ctx context.Context, log ledger.Log) error

func (fn ExportWriterFn) Write(ctx context.Context, log ledger.Log) error {
	return fn(ctx, log)
}
