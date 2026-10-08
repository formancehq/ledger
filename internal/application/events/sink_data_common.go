package events

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

// ---------- Common time type for analytical sinks ----------

// sinkTime wraps time.Time to serialize as "YYYY-MM-DD HH:MM:SS.ffffff"
// which is the format required by ClickHouse's JSON column parser and also
// suitable for Databricks STRING columns.
type sinkTime time.Time

const sinkTimeFormat = "2006-01-02 15:04:05.999999"

func (t sinkTime) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"%s"`, time.Time(t).UTC().Format(sinkTimeFormat)), nil
}

// ---------- Go structs for analytical sink JSON ----------

// sinkEventData is the flattened event payload stored in analytical sinks.
// Fields are populated based on the event type; unused fields are omitted.
type sinkEventData struct {
	// COMMITTED_TRANSACTION
	Transaction     *sinkTransaction             `json:"transaction,omitempty"`
	AccountMetadata map[string]map[string]string `json:"accountMetadata,omitempty"`

	// REVERTED_TRANSACTION
	RevertedTransactionID *uint64          `json:"revertedTransactionId,omitempty"`
	RevertTransaction     *sinkTransaction `json:"revertTransaction,omitempty"`

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

	// SKIPPED_ORDER
	SkippedReason  *string           `json:"skippedReason,omitempty"`
	SkippedContext map[string]string `json:"skippedContext,omitempty"`
}

type sinkTransaction struct {
	ID         uint64            `json:"id"`
	Postings   []sinkPosting     `json:"postings"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Reference  string            `json:"reference,omitempty"`
	Timestamp  sinkTime          `json:"timestamp"`
	Reverted   bool              `json:"reverted"`
	InsertedAt sinkTime          `json:"insertedAt"`
}

type sinkPosting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
	// Color is always emitted (no `omitempty`) so downstream analytical
	// consumers can distinguish the uncolored bucket (`color:""`) from an
	// older payload that predates the dimension — same contract as
	// ledgerpb.Posting.MarshalJSON, VolumeEntry, accountVolumeJSON.
	Color string `json:"color"`
}

// ---------- Conversion from Event protobuf ----------

// eventToSinkJSON converts an Event protobuf into JSON suitable for analytical
// sinks (ClickHouse, Databricks). The output uses proper Go types so
// encoding/json produces values that match the expected column types.
func eventToSinkJSON(event *eventspb.Event) ([]byte, error) {
	data := sinkEventData{}

	log := event.GetLog()
	if log == nil {
		return json.Marshal(data)
	}

	if log.GetPayload() == nil {
		return json.Marshal(data)
	}

	switch p := log.GetPayload().GetType().(type) {
	case *ledgerpb.LogPayload_CreateLedger:
		data.LedgerName = &p.CreateLedger.Name

	case *ledgerpb.LogPayload_DeleteLedger:
		data.LedgerName = &p.DeleteLedger.Name

	case *ledgerpb.LogPayload_Apply:
		sinkPopulateApply(&data, p.Apply)

	case *ledgerpb.LogPayload_RegisterSigningKey:
		data.SigningKeyID = &p.RegisterSigningKey.KeyId
		pk := hex.EncodeToString(p.RegisterSigningKey.GetPublicKey())
		data.PublicKey = &pk

	case *ledgerpb.LogPayload_RevokeSigningKey:
		data.SigningKeyID = &p.RevokeSigningKey.KeyId

	case *ledgerpb.LogPayload_SetSigningConfig:
		data.RequireSignatures = &p.SetSigningConfig.RequireSignatures

	case *ledgerpb.LogPayload_AddedEventsSink:
		if p.AddedEventsSink.GetConfig() != nil {
			data.SinkName = &p.AddedEventsSink.Config.Name
		}

	case *ledgerpb.LogPayload_RemovedEventsSink:
		data.SinkName = &p.RemovedEventsSink.Name
	}

	return json.Marshal(data)
}

// ---------- Helpers ----------

func sinkPopulateApply(data *sinkEventData, apply *ledgerpb.ApplyLedgerLog) {
	if apply == nil || apply.GetLog() == nil || apply.GetLog().GetData() == nil {
		return
	}

	switch lp := apply.GetLog().GetData().GetPayload().(type) {
	case *ledgerpb.LedgerLogPayload_CreatedTransaction:
		data.Transaction = sinkConvertTransaction(lp.CreatedTransaction.GetTransaction())
		data.AccountMetadata = sinkConvertAccountMetadataMap(lp.CreatedTransaction.GetAccountMetadata())

	case *ledgerpb.LedgerLogPayload_RevertedTransaction:
		data.RevertedTransactionID = &lp.RevertedTransaction.RevertedTransactionId
		data.RevertTransaction = sinkConvertTransaction(lp.RevertedTransaction.GetRevertTransaction())

	case *ledgerpb.LedgerLogPayload_SavedMetadata:
		data.TargetType, data.TargetID = sinkConvertTarget(lp.SavedMetadata.GetTarget())
		data.Metadata = sinkConvertMetadata(lp.SavedMetadata.GetMetadata())

	case *ledgerpb.LedgerLogPayload_DeletedMetadata:
		data.TargetType, data.TargetID = sinkConvertTarget(lp.DeletedMetadata.GetTarget())
		data.Key = &lp.DeletedMetadata.Key

	case *ledgerpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema operations — no sink-specific data
	case *ledgerpb.LedgerLogPayload_RemovedMetadataFieldType:
		// Schema operations — no sink-specific data

	case *ledgerpb.LedgerLogPayload_OrderSkipped:
		// Serialize the short public identifier (e.g. TRANSACTION_REFERENCE_CONFLICT)
		// rather than the generated enum name (ERROR_REASON_...) so sink consumers
		// can match against the same values callers submit in skippableReasons and
		// that the REST/bulk response and OrderSkippedLog JSON already expose.
		reason := domain.ReasonString(lp.OrderSkipped.GetReason())
		data.SkippedReason = &reason
		data.SkippedContext = lp.OrderSkipped.GetContext()
	}
}

func sinkConvertTransaction(tx *ledgerpb.Transaction) *sinkTransaction {
	if tx == nil {
		return nil
	}

	result := &sinkTransaction{
		ID:        tx.GetId(),
		Reference: tx.GetReference(),
		Reverted:  tx.GetReverted(),
	}

	if tx.GetTimestamp() != nil {
		result.Timestamp = sinkTime(tx.GetTimestamp().AsTime())
	}

	if tx.GetInsertedAt() != nil {
		result.InsertedAt = sinkTime(tx.GetInsertedAt().AsTime())
	}

	result.Postings = make([]sinkPosting, len(tx.GetPostings()))
	for i, p := range tx.GetPostings() {
		result.Postings[i] = sinkPosting{
			Source:      p.GetSource(),
			Destination: p.GetDestination(),
			Asset:       p.GetAsset(),
			Color:       p.GetColor(),
			Amount:      p.GetAmount().ToBigInt(),
		}
	}

	result.Metadata = sinkConvertMetadata(tx.GetMetadata())

	return result
}

func sinkConvertMetadata(m map[string]*ledgerpb.MetadataValue) map[string]string {
	if len(m) == 0 {
		return nil
	}

	result := make(map[string]string, len(m))
	for key, value := range m {
		if value != nil {
			result[key] = protohelpers.MetadataValueToString(value)
		}
	}

	return result
}

func sinkConvertAccountMetadataMap(am map[string]*ledgerpb.MetadataMap) map[string]map[string]string {
	if len(am) == 0 {
		return nil
	}

	result := make(map[string]map[string]string, len(am))
	for addr, mm := range am {
		result[addr] = sinkConvertMetadata(mm.GetValues())
	}

	return result
}

func sinkConvertTarget(target *ledgerpb.Target) (*string, any) {
	if target == nil {
		return nil, nil
	}

	switch t := target.GetTarget().(type) {
	case *ledgerpb.Target_Account:
		tt := "account"

		return &tt, t.Account.GetAddr()
	case *ledgerpb.Target_TransactionId:
		tt := "transaction"

		return &tt, t.TransactionId
	default:
		return nil, nil
	}
}
