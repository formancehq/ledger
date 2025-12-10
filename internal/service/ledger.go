package service

import (
	"context"
	"encoding/json"
	"fmt"
	stdtime "time"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type Ledger interface {
	CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error)
	RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error)
	SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error)
	SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error)
	DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error)
	DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error)
	Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error
	Export(ctx context.Context, ledgerName string, w ExportWriter) error
}

type Parameters[INPUT any] struct {
	DryRun         bool   `json:"dryRun,omitempty"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Input          INPUT  `json:"-"`
}

type TransactionScript struct {
	Plain string            `json:"plain"`
	Vars  map[string]string `json:"vars,omitempty"`
}

type CreateTransaction struct {
	AccountMetadata map[string]metadata.Metadata `json:"accountMetadata,omitempty"`
	Timestamp       *time.Time                   `json:"timestamp,omitempty"`
	Metadata        metadata.Metadata            `json:"metadata,omitempty"`
	Reference       string                       `json:"reference,omitempty"`
	Postings        ledger.Postings              `json:"postings,omitempty"`
	Script          *TransactionScript           `json:"script,omitempty"`
	Runtime         string                       `json:"runtime,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler for CreateTransaction
// It handles conversion of timestamp from string to *time.Time and metadata from map[string]interface{} to metadata.Metadata
func (c *CreateTransaction) UnmarshalJSON(data []byte) error {
	var aux struct {
		AccountMetadata map[string]map[string]interface{} `json:"accountMetadata,omitempty"`
		Timestamp       string                            `json:"timestamp,omitempty"`
		Metadata        map[string]interface{}            `json:"metadata,omitempty"`
		Reference       string                            `json:"reference,omitempty"`
		Postings        ledger.Postings                   `json:"postings,omitempty"`
		Script          *TransactionScript                `json:"script,omitempty"`
		Runtime         string                            `json:"runtime,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Convert account metadata
	c.AccountMetadata = make(map[string]metadata.Metadata)
	for addr, md := range aux.AccountMetadata {
		c.AccountMetadata[addr] = convertMapToMetadata(md)
	}

	// Convert timestamp
	if aux.Timestamp != "" {
		parsedTime, err := stdtime.Parse(stdtime.RFC3339, aux.Timestamp)
		if err != nil {
			return err
		}
		t := time.New(parsedTime)
		c.Timestamp = &t
	}

	// Convert metadata
	c.Metadata = convertMapToMetadata(aux.Metadata)
	c.Reference = aux.Reference
	c.Postings = aux.Postings
	c.Script = aux.Script
	c.Runtime = aux.Runtime
	return nil
}

// convertMapToMetadata converts map[string]interface{} to metadata.Metadata
func convertMapToMetadata(m map[string]interface{}) metadata.Metadata {
	if m == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range m {
		md[k] = fmt.Sprintf("%v", v)
	}
	return md
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
