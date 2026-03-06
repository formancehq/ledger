//go:build clickhouse

package events

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
)

// ---------- ClickHouse JSON sub-column DDL (DRY) ----------

// clickhouseTransactionColumns defines the typed sub-columns for a transaction
// inside the JSON column. Reused for both `transaction` and `revertTransaction`.
const clickhouseTransactionColumns = `JSON(
            id UInt64,
            postings Array(JSON(
                source String,
                destination String,
                amount UInt256,
                asset String
            )),
            metadata Map(String, String),
            reference Nullable(String),
            timestamp DateTime64(6, 'UTC'),
            reverted Bool,
            insertedAt DateTime64(6, 'UTC')
        )`

// ClickHouseCreateTableDDL returns the CREATE TABLE statement for the events table
// with a fully-typed JSON column matching the ledger v2 reference implementation.
func ClickHouseCreateTableDDL(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    log_sequence UInt64,
    type         LowCardinality(String),
    ledger       LowCardinality(String),
    date         DateTime64(6, 'UTC'),
    data         JSON(
        transaction %s,
        accountMetadata Map(String, String),
        revertedTransactionId Nullable(UInt64),
        revertTransaction %s,
        targetType Nullable(String),
        targetId Variant(UInt64, String),
        metadata Map(String, String),
        key Nullable(String),
        ledgerName Nullable(String),
        signingKeyId Nullable(String),
        publicKey Nullable(String),
        requireSignatures Nullable(Bool),
        sinkName Nullable(String),
        hash Nullable(String),
        idempotencyKey Nullable(String)
    )
) ENGINE = MergeTree()
ORDER BY (ledger, log_sequence)`, table, clickhouseTransactionColumns, clickhouseTransactionColumns)
}

// ---------- ClickHouse-compatible time type ----------

// clickhouseTime wraps time.Time to serialize as "YYYY-MM-DD HH:MM:SS.ffffff"
// which is the format ClickHouse's JSON column parser accepts for DateTime64.
// Go's default time.Time marshals as RFC3339 ("2006-01-02T15:04:05Z") which
// ClickHouse cannot parse inside JSON columns.
type clickhouseTime time.Time

const clickhouseTimeFormat = "2006-01-02 15:04:05.999999"

func (t clickhouseTime) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, time.Time(t).UTC().Format(clickhouseTimeFormat)), nil
}

// ---------- Go structs for ClickHouse-friendly JSON ----------

// clickhouseEventData is the flattened event payload stored in the JSON column.
// Fields are populated based on the event type; unused fields are omitted from
// JSON output so ClickHouse stores only the relevant sub-columns per row.
type clickhouseEventData struct {
	// COMMITTED_TRANSACTION
	Transaction     *clickhouseTransaction       `json:"transaction,omitempty"`
	AccountMetadata map[string]map[string]string `json:"accountMetadata,omitempty"`

	// REVERTED_TRANSACTION
	RevertedTransactionID *uint64                `json:"revertedTransactionId,omitempty"`
	RevertTransaction     *clickhouseTransaction `json:"revertTransaction,omitempty"`

	// SAVED_METADATA / DELETED_METADATA
	TargetType *string           `json:"targetType,omitempty"`
	TargetID   any               `json:"targetId,omitempty"` // string (account) or uint64 (transaction)
	Metadata   map[string]string `json:"metadata,omitempty"`
	Key        *string           `json:"key,omitempty"`

	// CREATED_LEDGER / DELETED_LEDGER
	LedgerName *string `json:"ledgerName,omitempty"`

	// Signing key management
	SigningKeyID      *string `json:"signingKeyId,omitempty"`
	PublicKey         *string `json:"publicKey,omitempty"`
	RequireSignatures *bool   `json:"requireSignatures,omitempty"`

	// Sink events
	SinkName *string `json:"sinkName,omitempty"`

	// Audit trail
	Hash           *string `json:"hash,omitempty"`
	IdempotencyKey *string `json:"idempotencyKey,omitempty"`
}

type clickhouseTransaction struct {
	ID         uint64              `json:"id"`
	Postings   []clickhousePosting `json:"postings"`
	Metadata   map[string]string   `json:"metadata,omitempty"`
	Reference  string              `json:"reference,omitempty"`
	Timestamp  clickhouseTime      `json:"timestamp"`
	Reverted   bool                `json:"reverted"`
	InsertedAt clickhouseTime      `json:"insertedAt"`
}

type clickhousePosting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
}

// ---------- Conversion from Event protobuf ----------

// eventToClickHouseJSON converts an Event protobuf into ClickHouse-friendly JSON
// with proper Go types so encoding/json produces values that match the declared
// ClickHouse JSON sub-column types (UInt64, DateTime64, UInt256, etc.).
func eventToClickHouseJSON(event *eventspb.Event) ([]byte, error) {
	data := clickhouseEventData{}

	log := event.GetLog()
	if log == nil {
		return json.Marshal(data)
	}

	// Audit trail
	if len(log.GetHash()) > 0 {
		h := hex.EncodeToString(log.GetHash())
		data.Hash = &h
	}

	if log.GetIdempotency() != nil && log.GetIdempotency().GetKey() != "" {
		data.IdempotencyKey = &log.Idempotency.Key
	}

	if log.GetPayload() == nil {
		return json.Marshal(data)
	}

	switch p := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_CreateLedger:
		populateLedgerInfo(&data, p.CreateLedger.GetInfo())

	case *commonpb.LogPayload_DeleteLedger:
		populateLedgerInfo(&data, p.DeleteLedger.GetInfo())

	case *commonpb.LogPayload_Apply:
		populateApply(&data, p.Apply)

	case *commonpb.LogPayload_RegisterSigningKey:
		data.SigningKeyID = &p.RegisterSigningKey.KeyId
		pk := hex.EncodeToString(p.RegisterSigningKey.GetPublicKey())
		data.PublicKey = &pk

	case *commonpb.LogPayload_RevokeSigningKey:
		data.SigningKeyID = &p.RevokeSigningKey.KeyId

	case *commonpb.LogPayload_SetSigningConfig:
		data.RequireSignatures = &p.SetSigningConfig.RequireSignatures

	case *commonpb.LogPayload_AddedEventsSink:
		if p.AddedEventsSink.GetConfig() != nil {
			data.SinkName = &p.AddedEventsSink.Config.Name
		}

	case *commonpb.LogPayload_RemovedEventsSink:
		data.SinkName = &p.RemovedEventsSink.Name
	}

	return json.Marshal(data)
}

// ---------- Helpers ----------

func populateLedgerInfo(data *clickhouseEventData, info *commonpb.LedgerInfo) {
	if info == nil {
		return
	}

	data.LedgerName = &info.Name
}

func populateApply(data *clickhouseEventData, apply *commonpb.ApplyLedgerLog) {
	if apply == nil || apply.GetLog() == nil || apply.GetLog().GetData() == nil {
		return
	}

	switch lp := apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		data.Transaction = convertTransaction(lp.CreatedTransaction.GetTransaction())
		data.AccountMetadata = convertAccountMetadataMap(lp.CreatedTransaction.GetAccountMetadata())

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		data.RevertedTransactionID = &lp.RevertedTransaction.RevertedTransactionId
		data.RevertTransaction = convertTransaction(lp.RevertedTransaction.GetRevertTransaction())

	case *commonpb.LedgerLogPayload_SavedMetadata:
		data.TargetType, data.TargetID = convertTarget(lp.SavedMetadata.GetTarget())
		data.Metadata = convertMetadataSet(lp.SavedMetadata.GetMetadata())

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		data.TargetType, data.TargetID = convertTarget(lp.DeletedMetadata.GetTarget())
		data.Key = &lp.DeletedMetadata.Key

	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema operations — no ClickHouse-specific data
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		// Schema operations — no ClickHouse-specific data
	case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
		// Background conversion — no ClickHouse-specific data
	case *commonpb.LedgerLogPayload_MetadataConversionComplete:
		// Background conversion — no ClickHouse-specific data
	}
}

func convertTransaction(tx *commonpb.Transaction) *clickhouseTransaction {
	if tx == nil {
		return nil
	}

	result := &clickhouseTransaction{
		ID:        tx.GetId(),
		Reference: tx.GetReference(),
		Reverted:  tx.GetReverted(),
	}

	if tx.GetTimestamp() != nil {
		result.Timestamp = clickhouseTime(tx.GetTimestamp().AsTime().Time)
	}

	if tx.GetInsertedAt() != nil {
		result.InsertedAt = clickhouseTime(tx.GetInsertedAt().AsTime().Time)
	}

	result.Postings = make([]clickhousePosting, len(tx.GetPostings()))
	for i, p := range tx.GetPostings() {
		result.Postings[i] = clickhousePosting{
			Source:      p.GetSource(),
			Destination: p.GetDestination(),
			Asset:       p.GetAsset(),
			Amount:      p.GetAmount().ToBigInt(),
		}
	}

	result.Metadata = convertMetadataSet(tx.GetMetadata())

	return result
}

func convertMetadataSet(ms *commonpb.MetadataSet) map[string]string {
	if ms == nil || len(ms.GetMetadata()) == 0 {
		return nil
	}

	result := make(map[string]string, len(ms.GetMetadata()))
	for _, entry := range ms.GetMetadata() {
		if entry.GetValue() != nil {
			result[entry.GetKey()] = commonpb.MetadataValueToString(entry.GetValue())
		}
	}

	return result
}

func convertAccountMetadataMap(am map[string]*commonpb.MetadataSet) map[string]map[string]string {
	if len(am) == 0 {
		return nil
	}

	result := make(map[string]map[string]string, len(am))
	for addr, ms := range am {
		result[addr] = convertMetadataSet(ms)
	}

	return result
}

func convertTarget(target *commonpb.Target) (*string, any) {
	if target == nil {
		return nil, nil
	}

	switch t := target.GetTarget().(type) {
	case *commonpb.Target_Account:
		tt := "account"

		return &tt, t.Account.GetAddr()
	case *commonpb.Target_Transaction:
		tt := "transaction"

		return &tt, t.Transaction.GetId()
	default:
		return nil, nil
	}
}
