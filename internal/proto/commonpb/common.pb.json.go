package commonpb

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// protoFieldJSON marshals a proto.Message field to json.RawValue using protojson,
// preserving camelCase field names. Returns nil for nil/zero messages.
func protoFieldJSON(msg proto.Message) json.RawValue {
	if msg == nil {
		return nil
	}

	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil
	}

	return b
}

// Note: Transaction.MarshalJSON is already implemented in transaction.go

// MarshalJSON implements json.Marshaler for Log (global log).
func (x *Log) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Sequence          uint64        `json:"sequence,omitempty"`
		Payload           *LogPayload   `json:"payload,omitempty"`
		Receipt           string        `json:"receipt,omitempty"`
		ResponseSignature json.RawValue `json:"responseSignature,omitempty"`
	}

	aux := Aux{
		Sequence:          x.GetSequence(),
		Payload:           x.GetPayload(),
		ResponseSignature: protoFieldJSON(x.GetResponseSignature()),
		Receipt:           x.GetReceipt(),
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for LogPayload (oneof dispatch).
func (x *LogPayload) MarshalJSON() ([]byte, error) {
	switch p := x.GetType().(type) {
	case *LogPayload_CreateLedger:
		return json.Marshal(&struct {
			CreateLedger *CreatedLedgerLog `json:"createLedger,omitempty"`
		}{CreateLedger: p.CreateLedger})
	case *LogPayload_DeleteLedger:
		return json.Marshal(&struct {
			DeleteLedger *DeletedLedgerLog `json:"deleteLedger,omitempty"`
		}{DeleteLedger: p.DeleteLedger})
	case *LogPayload_Apply:
		return json.Marshal(&struct {
			Apply *ApplyLedgerLog `json:"apply,omitempty"`
		}{Apply: p.Apply})
	default:
		// Other variants (signing, sinks, chapters, etc.) — use protojson for camelCase
		return protojson.Marshal(x)
	}
}

// MarshalJSON implements json.Marshaler for CreatedLedgerLog.
func (x *CreatedLedgerLog) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name                   string                  `json:"name,omitempty"`
		CreatedAt              *Timestamp              `json:"createdAt,omitempty"`
		MetadataSchema         *MetadataSchema         `json:"metadataSchema,omitempty"`
		Mode                   LedgerMode              `json:"mode,omitempty"`
		MirrorSource           *MirrorSourceConfig     `json:"mirrorSource,omitempty"`
		AccountTypes           map[string]*AccountType `json:"accountTypes,omitempty"`
		DefaultEnforcementMode ChartEnforcementMode    `json:"defaultEnforcementMode,omitempty"`
	}{
		Name:                   x.GetName(),
		CreatedAt:              x.GetCreatedAt(),
		MetadataSchema:         x.GetMetadataSchema(),
		Mode:                   x.GetMode(),
		MirrorSource:           x.GetMirrorSource(),
		AccountTypes:           x.GetAccountTypes(),
		DefaultEnforcementMode: x.GetDefaultEnforcementMode(),
	})
}

// MarshalJSON implements json.Marshaler for DeletedLedgerLog.
func (x *DeletedLedgerLog) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name      string     `json:"name,omitempty"`
		DeletedAt *Timestamp `json:"deletedAt,omitempty"`
	}{
		Name:      x.GetName(),
		DeletedAt: x.GetDeletedAt(),
	})
}

// MarshalJSON implements json.Marshaler for ApplyLedgerLog.
func (x *ApplyLedgerLog) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		LedgerName string     `json:"ledgerName,omitempty"`
		Log        *LedgerLog `json:"log,omitempty"`
	}{
		LedgerName: x.GetLedgerName(),
		Log:        x.GetLog(),
	})
}

// MarshalJSON implements json.Marshaler for LedgerLogPayload (oneof dispatch).
func (x *LedgerLogPayload) MarshalJSON() ([]byte, error) {
	switch p := x.GetPayload().(type) {
	case *LedgerLogPayload_CreatedTransaction:
		return json.Marshal(&struct {
			CreatedTransaction *CreatedTransaction `json:"createdTransaction,omitempty"`
		}{CreatedTransaction: p.CreatedTransaction})
	case *LedgerLogPayload_RevertedTransaction:
		return json.Marshal(&struct {
			RevertedTransaction *RevertedTransaction `json:"revertedTransaction,omitempty"`
		}{RevertedTransaction: p.RevertedTransaction})
	case *LedgerLogPayload_SavedMetadata:
		return json.Marshal(&struct {
			SavedMetadata *SavedMetadata `json:"savedMetadata,omitempty"`
		}{SavedMetadata: p.SavedMetadata})
	case *LedgerLogPayload_DeletedMetadata:
		return json.Marshal(&struct {
			DeletedMetadata *DeletedMetadata `json:"deletedMetadata,omitempty"`
		}{DeletedMetadata: p.DeletedMetadata})
	default:
		// Other variants — use protojson for camelCase
		return protojson.Marshal(x)
	}
}

// MarshalJSON implements json.Marshaler for PostCommitVolumes. The wire shape
// is a flat `{address: {asset: Volumes}}` map — protojson would emit the raw
// proto wrappers (`volumesByAccount.{addr}.volumes.{asset}`), leaking two
// nesting levels that don't belong on the public API and don't match the
// OpenAPI schema (see PostCommitVolumes in openapi.yml).
func (x *PostCommitVolumes) MarshalJSON() ([]byte, error) {
	byAccount := x.GetVolumesByAccount()
	if len(byAccount) == 0 {
		return []byte("{}"), nil
	}

	flat := make(map[string]map[string]*Volumes, len(byAccount))
	for addr, va := range byAccount {
		flat[addr] = va.GetVolumes()
	}

	return json.Marshal(flat)
}

// (No custom UnmarshalJSON for PostCommitVolumes.) The type is response-only:
// the server emits it, no request payload ever carries it, so there is no
// production caller for reverse conversion. Client-side consumers wanting to
// parse the flat shape can decode straight into a
// `map[string]map[string]Volumes` — the same structure MarshalJSON emits.

// MarshalJSON implements json.Marshaler for Account.
func (x *Account) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Address       string         `json:"address,omitempty"`
		Metadata      map[string]any `json:"metadata,omitempty"`
		FirstUsage    *Timestamp     `json:"firstUsage,omitempty"`
		InsertionDate *Timestamp     `json:"insertionDate,omitempty"`
		UpdatedAt     *Timestamp     `json:"updatedAt,omitempty"`
	}{
		Address:       x.GetAddress(),
		Metadata:      MetadataToAnyMap(x.GetMetadata()),
		FirstUsage:    x.GetFirstUsage(),
		InsertionDate: x.GetInsertionDate(),
		UpdatedAt:     x.GetUpdatedAt(),
	})
}

// Note: Log.MarshalJSON is already implemented in log.go

// MarshalJSON implements json.Marshaler for CreatedTransaction.
func (x *CreatedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction       *Transaction              `json:"transaction,omitempty"`
		AccountMetadata   map[string]map[string]any `json:"accountMetadata,omitempty"`
		ChapterID         uint64                    `json:"chapterId,omitempty"`
		PostCommitVolumes *PostCommitVolumes        `json:"postCommitVolumes,omitempty"`
	}{
		Transaction:       x.GetTransaction(),
		AccountMetadata:   AccountMetadataToAnyMap(x.GetAccountMetadata()),
		ChapterID:         x.GetChapterId(),
		PostCommitVolumes: x.GetPostCommitVolumes(),
	})
}

// MarshalJSON implements json.Marshaler for RevertedTransaction.
func (x *RevertedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		RevertedTransactionID uint64             `json:"revertedTransactionId,omitempty"`
		RevertTransaction     *Transaction       `json:"revertTransaction,omitempty"`
		PostCommitVolumes     *PostCommitVolumes `json:"postCommitVolumes,omitempty"`
	}{
		RevertedTransactionID: x.GetRevertedTransactionId(),
		RevertTransaction:     x.GetRevertTransaction(),
		PostCommitVolumes:     x.GetPostCommitVolumes(),
	})
}

// MarshalJSON implements json.Marshaler for SavedMetadata.
func (x *SavedMetadata) MarshalJSON() ([]byte, error) {
	aux := struct {
		TargetType    string         `json:"targetType,omitempty"`
		AccountId     string         `json:"accountId,omitempty"`
		TransactionId uint64         `json:"transactionId,omitempty"`
		Metadata      map[string]any `json:"metadata,omitempty"`
	}{
		TargetType: x.GetTarget().AsConst(),
		Metadata:   MetadataToAnyMap(x.GetMetadata()),
	}

	// Handle oneof target_id
	switch v := x.GetTarget().GetTarget().(type) {
	case *Target_Account:
		aux.AccountId = v.Account.GetAddr()
	case *Target_TransactionId:
		aux.TransactionId = v.TransactionId
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for DeletedMetadata.
func (x *DeletedMetadata) MarshalJSON() ([]byte, error) {
	aux := struct {
		TargetType    string `json:"targetType,omitempty"`
		AccountId     string `json:"accountId,omitempty"`
		TransactionId uint64 `json:"transactionId,omitempty"`
		Key           string `json:"key,omitempty"`
	}{
		TargetType: x.GetTarget().AsConst(),
		Key:        x.GetKey(),
	}

	// Handle oneof target_id
	switch v := x.GetTarget().GetTarget().(type) {
	case *Target_Account:
		aux.AccountId = v.Account.GetAddr()
	case *Target_TransactionId:
		aux.TransactionId = v.TransactionId
	}

	return json.Marshal(aux)
}

// UnmarshalJSON implements json.Unmarshaler for DeletedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION).
func (dm *DeletedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string        `json:"targetType"`
		TargetID   json.RawValue `json:"targetId"`
		Key        string        `json:"key"`
	}

	x := X{}

	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	dm.Key = x.Key

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string

		err = json.Unmarshal(x.TargetID, &accountID)
		if err == nil {
			dm.Target = &Target{
				Target: &Target_Account{
					Account: &TargetAccount{
						Addr: accountID,
					},
				},
			}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64

		txID, err = strconv.ParseUint(string(x.TargetID), 10, 64)
		if err == nil {
			dm.Target = &Target{
				Target: &Target_TransactionId{TransactionId: txID},
			}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}

	return err
}

// UnmarshalJSON implements json.Unmarshaler for SavedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION).
func (sm *SavedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string         `json:"targetType"`
		TargetID   json.RawValue  `json:"targetId"`
		Metadata   map[string]any `json:"metadata"`
	}

	x := X{}

	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	md, err := MetadataFromAnyMap(x.Metadata)
	if err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	sm.Metadata = md

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string

		err = json.Unmarshal(x.TargetID, &accountID)
		if err == nil {
			sm.Target = &Target{
				Target: &Target_Account{
					Account: &TargetAccount{
						Addr: accountID,
					},
				},
			}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64

		txID, err = strconv.ParseUint(string(x.TargetID), 10, 64)
		if err == nil {
			sm.Target = &Target{
				Target: &Target_TransactionId{TransactionId: txID},
			}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}

	return err
}

// MarshalJSON implements json.Marshaler for PreparedQuery.
//
// PreparedQuery embeds a *QueryFilter (a protobuf oneof) and exposes a
// QueryTarget enum. Default encoding/json has no way to dispatch the oneof
// variants (their Go fields carry only `protobuf:",oneof"` — no `json:` tag)
// and emits the enum as a raw int. Result: `{"filter":{"Filter":{"Reference":
// {"cond":{"Value":{"Hardcoded":"..."}}}}},"target":1}` instead of the
// camelCase shape the REST contract advertises and that the input side
// already accepts via `decodePreparedQueryFilter` (protojson).
//
// Route the whole value through protojson to keep the response symmetric
// with the request — same class of bug as #459 / #473.
func (x *PreparedQuery) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(x)
}

// MarshalJSON implements json.Marshaler for PreparedQueryCursor.
func (x *PreparedQueryCursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		PageSize        uint32         `json:"pageSize"`
		HasMore         bool           `json:"hasMore"`
		Previous        string         `json:"previous,omitempty"`
		Next            string         `json:"next,omitempty"`
		AccountData     []*Account     `json:"accountData,omitempty"`
		TransactionData []*Transaction `json:"transactionData,omitempty"`
	}{
		PageSize:        x.GetPageSize(),
		HasMore:         x.GetHasMore(),
		Previous:        x.GetPrevious(),
		Next:            x.GetNext(),
		AccountData:     x.GetAccountData(),
		TransactionData: x.GetTransactionData(),
	})
}

// MarshalJSON implements json.Marshaler for LedgerInfo.
func (x *LedgerInfo) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Name                   string        `json:"name,omitempty"`
		CreatedAt              *time.Time    `json:"createdAt,omitempty"`
		DeletedAt              *time.Time    `json:"deletedAt,omitempty"`
		MetadataSchema         json.RawValue `json:"metadataSchema,omitempty"`
		Mode                   string        `json:"mode,omitempty"`
		MirrorSource           json.RawValue `json:"mirrorSource,omitempty"`
		MirrorSyncProgress     json.RawValue `json:"mirrorSyncProgress,omitempty"`
		AccountTypes           json.RawValue `json:"accountTypes,omitempty"`
		DefaultEnforcementMode string        `json:"defaultEnforcementMode,omitempty"`
		Metadata               json.RawValue `json:"metadata,omitempty"`
	}

	aux := Aux{
		Name:                   x.GetName(),
		MetadataSchema:         protoFieldJSON(x.GetMetadataSchema()),
		MirrorSource:           protoFieldJSON(x.GetMirrorSource()),
		MirrorSyncProgress:     protoFieldJSON(x.GetMirrorSyncProgress()),
		DefaultEnforcementMode: x.GetDefaultEnforcementMode().String(),
	}

	if x.GetMode() != LedgerMode_LEDGER_MODE_NORMAL {
		aux.Mode = x.GetMode().String()
	}

	if x.GetCreatedAt() != nil {
		t := x.GetCreatedAt().AsTime()
		aux.CreatedAt = &t
	}

	if x.GetDeletedAt() != nil {
		t := x.GetDeletedAt().AsTime()
		aux.DeletedAt = &t
	}

	if len(x.GetAccountTypes()) > 0 {
		// Use protojson for the map of proto types to preserve camelCase
		m := make(map[string]json.RawValue, len(x.GetAccountTypes()))
		for k, v := range x.GetAccountTypes() {
			m[k] = protoFieldJSON(v)
		}

		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}

		aux.AccountTypes = b
	}

	if len(x.GetMetadata()) > 0 {
		b, err := json.Marshal(MetadataToAnyMap(x.GetMetadata()))
		if err != nil {
			return nil, err
		}

		aux.Metadata = b
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for NumscriptInfo.
//
// The protoc-gen-go struct tags use snake_case (created_at), so a default
// encoding/json marshal would emit `created_at` and break the camelCase REST
// contract that every other endpoint follows. Same class of bug as #459 for
// CreatedTransaction / RevertedTransaction / RevertTransactionPayload.
func (x *NumscriptInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name      string     `json:"name,omitempty"`
		Content   string     `json:"content,omitempty"`
		Version   string     `json:"version,omitempty"`
		CreatedAt *Timestamp `json:"createdAt,omitempty"`
		Ledger    string     `json:"ledger,omitempty"`
	}{
		Name:      x.GetName(),
		Content:   x.GetContent(),
		Version:   x.GetVersion(),
		CreatedAt: x.GetCreatedAt(),
		Ledger:    x.GetLedger(),
	})
}

// MarshalJSON implements json.Marshaler for Chapter.
func (x *Chapter) MarshalJSON() ([]byte, error) {
	type Aux struct {
		ID                 uint64     `json:"id,omitempty"`
		Start              *time.Time `json:"start,omitempty"`
		End                *time.Time `json:"end,omitempty"`
		Status             string     `json:"status,omitempty"`
		CloseSequence      uint64     `json:"closeSequence,omitempty"`
		SealingHash        string     `json:"sealingHash,omitempty"`
		LastAuditHash      string     `json:"lastAuditHash,omitempty"`
		StartSequence      uint64     `json:"startSequence,omitempty"`
		StateHash          string     `json:"stateHash,omitempty"`
		StartAuditSequence uint64     `json:"startAuditSequence,omitempty"`
		CloseAuditSequence uint64     `json:"closeAuditSequence,omitempty"`
	}

	aux := Aux{
		ID:                 x.GetId(),
		Status:             x.GetStatus().String(),
		CloseSequence:      x.GetCloseSequence(),
		StartSequence:      x.GetStartSequence(),
		StartAuditSequence: x.GetStartAuditSequence(),
		CloseAuditSequence: x.GetCloseAuditSequence(),
	}

	if x.GetStart() != nil {
		t := x.GetStart().AsTime()
		aux.Start = &t
	}

	if x.GetEnd() != nil {
		t := x.GetEnd().AsTime()
		aux.End = &t
	}

	if len(x.GetSealingHash()) > 0 {
		aux.SealingHash = hex.EncodeToString(x.GetSealingHash())
	}

	if len(x.GetLastAuditHash()) > 0 {
		aux.LastAuditHash = hex.EncodeToString(x.GetLastAuditHash())
	}

	if len(x.GetStateHash()) > 0 {
		aux.StateHash = hex.EncodeToString(x.GetStateHash())
	}

	return json.Marshal(aux)
}

// ParseTarget parses targetType and targetId/targetReference into a Target.
// Returns an error when the inputs cannot be parsed instead of silently
// returning nil — the caller should surface this to the client.
func ParseTarget(targetType string, targetID json.RawValue) (*Target, error) {
	switch strings.ToUpper(targetType) {
	case MetaTargetTypeAccount:
		if len(targetID) == 0 {
			return nil, errors.New("account target requires targetId")
		}

		var addr string
		if err := json.Unmarshal(targetID, &addr); err != nil {
			return nil, fmt.Errorf("account targetId must be a string: %w", err)
		}

		return &Target{
			Target: &Target_Account{
				Account: &TargetAccount{Addr: addr},
			},
		}, nil

	case MetaTargetTypeTransaction:
		if len(targetID) == 0 {
			return nil, errors.New("transaction target requires targetId")
		}

		var id uint64
		if err := json.Unmarshal(targetID, &id); err != nil {
			return nil, fmt.Errorf("transaction targetId must be a uint64: %w", err)
		}

		return &Target{
			Target: &Target_TransactionId{TransactionId: id},
		}, nil
	}

	return nil, fmt.Errorf("unsupported targetType %q", targetType)
}
