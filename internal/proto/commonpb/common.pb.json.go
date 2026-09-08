package commonpb

import (
	"errors"
	"fmt"
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
		ResponseSignature json.RawValue `json:"responseSignature,omitempty"`
	}

	aux := Aux{
		Sequence:          x.GetSequence(),
		Payload:           x.GetPayload(),
		ResponseSignature: protoFieldJSON(x.GetResponseSignature()),
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
		// Other variants (signing, sinks, etc.) — use protojson for camelCase
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
	case *LedgerLogPayload_OrderSkipped:
		return p.OrderSkipped.MarshalJSON()
	default:
		// Other variants — use protojson for camelCase
		return protojson.Marshal(x)
	}
}

// MarshalJSON implements json.Marshaler for OrderSkippedLog. Renders the
// ErrorReason as the SHORT identifier (e.g. "TRANSACTION_REFERENCE_CONFLICT")
// matching the wire convention used by the REST API surface
// (skippableReasons, OrderSkippedResponse.reason, gRPC ErrorInfo.reason).
// Standard encoding/json would emit the int enum value, breaking the
// HydrateLog round-trip through LedgerLog's JSON layer.
func (x *OrderSkippedLog) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Reason  string            `json:"reason"`
		Context map[string]string `json:"context,omitempty"`
	}{
		Reason:  strings.TrimPrefix(x.GetReason().String(), "ERROR_REASON_"),
		Context: x.GetContext(),
	})
}

// UnmarshalJSON implements json.Unmarshaler for OrderSkippedLog. Accepts
// the short reason identifier (e.g. "TRANSACTION_REFERENCE_CONFLICT") and
// re-prepends the "ERROR_REASON_" prefix before the enum-name lookup —
// keeps the JSON wire symmetric with MarshalJSON and consistent with the
// CreateTransactionPayload skippableReasons decoder.
func (x *OrderSkippedLog) UnmarshalJSON(data []byte) error {
	var aux struct {
		Reason  string            `json:"reason"`
		Context map[string]string `json:"context"`
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.Reason != "" {
		code, ok := ErrorReason_value["ERROR_REASON_"+aux.Reason]
		if !ok {
			return fmt.Errorf("unknown ErrorReason %q", aux.Reason)
		}

		x.Reason = ErrorReason(code)
	}

	x.Context = aux.Context

	return nil
}

// MarshalJSON implements json.Marshaler for PostCommitVolumes. The wire shape
// is a flat `{"addr": [{asset, color, input, output}]}` map — one array of
// (asset, color) tuples per account. protojson would otherwise emit the raw
// proto wrappers (`{"volumesByAccount": {"addr": {"volumes": [...]}}}`) two
// levels deep. This replaces the pre-color EN-1465 `{"addr": {"asset": Volumes}}`
// map shape, which can no longer key a bucket uniquely once a color dimension
// exists, while keeping the flatten intent (no protojson wrappers).
func (x *PostCommitVolumes) MarshalJSON() ([]byte, error) {
	byAccount := x.GetVolumesByAccount()
	if len(byAccount) == 0 {
		return []byte("{}"), nil
	}

	flat := make(map[string][]*VolumeEntry, len(byAccount))
	for addr, va := range byAccount {
		flat[addr] = va.GetVolumes()
	}

	return json.Marshal(flat)
}

// UnmarshalJSON reverses the flat account-to-volume-list response shape.
func (x *PostCommitVolumes) UnmarshalJSON(data []byte) error {
	var flat map[string][]*VolumeEntry
	if err := json.Unmarshal(data, &flat); err != nil {
		return err
	}
	x.VolumesByAccount = make(map[string]*VolumesByAssets, len(flat))
	for account, volumes := range flat {
		x.VolumesByAccount[account] = &VolumesByAssets{Volumes: volumes}
	}

	return nil
}

// UnmarshalJSON reverses the flat asset/color/input/output response tuple.
func (x *VolumeEntry) UnmarshalJSON(data []byte) error {
	var aux struct {
		Asset  string `json:"asset"`
		Color  string `json:"color"`
		Input  string `json:"input"`
		Output string `json:"output"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.Asset, x.Color = aux.Asset, aux.Color
	x.Volumes = &Volumes{Input: aux.Input, Output: aux.Output}

	return nil
}

// MarshalJSON implements json.Marshaler for VolumeEntry. Color is always
// emitted (even when empty) so clients can distinguish the uncolored bucket
// from an older response shape — same contract as accountVolumeJSON and
// aggregatedVolumeJSON in the REST handler layer. Input/output are flattened
// onto the tuple (not nested under a `volumes` key) so a post-commit-volume
// entry reads as one flat `{asset, color, input, output}` row.
func (x *VolumeEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Asset  string `json:"asset"`
		Color  string `json:"color"`
		Input  string `json:"input"`
		Output string `json:"output"`
	}{
		Asset:  x.GetAsset(),
		Color:  x.GetColor(),
		Input:  x.GetVolumes().GetInput(),
		Output: x.GetVolumes().GetOutput(),
	})
}

// accountVolumeJSON is the JSON shape for AccountVolume. Color is always
// emitted (even empty) because the API treats the empty bucket as a
// first-class entry and clients cannot otherwise tell "uncolored bucket"
// from "field absent in an older response shape".
type accountVolumeJSON struct {
	Asset   string              `json:"asset"`
	Color   string              `json:"color"`
	Volumes *VolumesWithBalance `json:"volumes,omitempty"`
}

// MarshalJSON implements json.Marshaler for Account.
func (x *Account) MarshalJSON() ([]byte, error) {
	volumes := make([]*accountVolumeJSON, 0, len(x.GetVolumes()))
	for _, v := range x.GetVolumes() {
		volumes = append(volumes, &accountVolumeJSON{
			Asset:   v.GetAsset(),
			Color:   v.GetColor(),
			Volumes: v.GetVolumes(),
		})
	}

	return json.Marshal(&struct {
		Address       string               `json:"address,omitempty"`
		Metadata      map[string]any       `json:"metadata,omitempty"`
		Volumes       []*accountVolumeJSON `json:"volumes"`
		FirstUsage    *Timestamp           `json:"firstUsage,omitempty"`
		InsertionDate *Timestamp           `json:"insertionDate,omitempty"`
		UpdatedAt     *Timestamp           `json:"updatedAt,omitempty"`
	}{
		Address:       x.GetAddress(),
		Metadata:      MetadataToAnyMap(x.GetMetadata()),
		Volumes:       volumes,
		FirstUsage:    x.GetFirstUsage(),
		InsertionDate: x.GetInsertionDate(),
		UpdatedAt:     x.GetUpdatedAt(),
	})
}

// Note: Log.MarshalJSON is already implemented in log.go

// MarshalJSON implements json.Marshaler for CreatedTransaction. Post-commit
// volumes ride on the embedded Transaction, so they surface via the
// "transaction" field rather than as a sibling here.
func (x *CreatedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction     *Transaction              `json:"transaction,omitempty"`
		AccountMetadata map[string]map[string]any `json:"accountMetadata,omitempty"`
	}{
		Transaction:     x.GetTransaction(),
		AccountMetadata: AccountMetadataToAnyMap(x.GetAccountMetadata()),
	})
}

// MarshalJSON implements json.Marshaler for RevertedTransaction. Post-commit
// volumes ride on the embedded revert Transaction, so they surface via the
// "revertTransaction" field rather than as a sibling here.
func (x *RevertedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		RevertedTransactionID uint64       `json:"revertedTransactionId,omitempty"`
		RevertTransaction     *Transaction `json:"revertTransaction,omitempty"`
	}{
		RevertedTransactionID: x.GetRevertedTransactionId(),
		RevertTransaction:     x.GetRevertTransaction(),
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

// UnmarshalJSON decodes the accountId/transactionId fields emitted by MarshalJSON.
func (dm *DeletedMetadata) UnmarshalJSON(data []byte) error {
	var aux struct {
		logMetadataTargetJSON

		Key string `json:"key"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	target, err := aux.target()
	if err != nil {
		return err
	}
	dm.Target, dm.Key = target, aux.Key

	return nil
}

// UnmarshalJSON decodes the target and typed response metadata emitted by MarshalJSON.
func (sm *SavedMetadata) UnmarshalJSON(data []byte) error {
	var aux struct {
		logMetadataTargetJSON

		Metadata logMetadataJSON `json:"metadata"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	target, err := aux.target()
	if err != nil {
		return err
	}
	sm.Target, sm.Metadata = target, aux.Metadata

	return nil
}

type logMetadataTargetJSON struct {
	TargetType    string `json:"targetType"`
	AccountID     string `json:"accountId"`
	TransactionID uint64 `json:"transactionId"`
}

func (x logMetadataTargetJSON) target() (*Target, error) {
	switch strings.ToUpper(x.TargetType) {
	case MetaTargetTypeAccount:
		return &Target{Target: &Target_Account{Account: &TargetAccount{Addr: x.AccountID}}}, nil
	case MetaTargetTypeTransaction:
		return &Target{Target: &Target_TransactionId{TransactionId: x.TransactionID}}, nil
	default:
		return nil, fmt.Errorf("unknown type %q", x.TargetType)
	}
}

// UnmarshalJSON reverses CreatedTransaction's camelCase response fields.
func (x *CreatedTransaction) UnmarshalJSON(data []byte) error {
	var aux struct {
		Transaction     *Transaction               `json:"transaction"`
		AccountMetadata map[string]logMetadataJSON `json:"accountMetadata"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.Transaction = aux.Transaction
	x.AccountMetadata = make(map[string]*MetadataMap, len(aux.AccountMetadata))
	for account, metadata := range aux.AccountMetadata {
		x.AccountMetadata[account] = &MetadataMap{Values: metadata}
	}

	return nil
}

// UnmarshalJSON reverses RevertedTransaction's camelCase response fields.
func (x *RevertedTransaction) UnmarshalJSON(data []byte) error {
	var aux struct {
		RevertedTransactionID uint64       `json:"revertedTransactionId"`
		RevertTransaction     *Transaction `json:"revertTransaction"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.RevertedTransactionId, x.RevertTransaction = aux.RevertedTransactionID, aux.RevertTransaction

	return nil
}

// MarshalJSON implements json.Marshaler for PreparedQuery.
//
// PreparedQuery embeds a *QueryFilter (a protobuf oneof) and exposes a
// QueryTarget enum. We deliberately do NOT route through protojson: that would
// leak the protobuf-internal oneof/wrapper names of QueryFilter onto the public
// REST surface (see the codec in query_filter.go). Instead the filter is
// encoded through QueryFilter's own hand-written MarshalJSON (canonical flat
// shape) and the target is emitted as the bare string enum.
//
// REST can create ACCOUNTS / TRANSACTIONS / LOGS prepared queries (see
// parsePreparedQueryTarget); the map covers all three proto values so the
// listed target is emitted faithfully regardless of how the query was created
// (REST or gRPC/CLI).
func (x *PreparedQuery) MarshalJSON() ([]byte, error) {
	target, ok := queryTargetToJSON[x.GetTarget()]
	if !ok {
		return nil, fmt.Errorf("prepared query: unknown target %v", x.GetTarget())
	}

	return json.Marshal(&struct {
		Name   string       `json:"name"`
		Target string       `json:"target"`
		Filter *QueryFilter `json:"filter,omitempty"`
	}{
		Name:   x.GetName(),
		Target: target,
		Filter: x.GetFilter(),
	})
}

// queryTargetToJSON maps the QueryTarget proto enum to the public string enum
// documented in openapi.yml. Kept local so the public contract never inherits
// the QUERY_TARGET_* proto prefixes.
var queryTargetToJSON = map[QueryTarget]string{
	QueryTarget_QUERY_TARGET_ACCOUNTS:     "ACCOUNTS",
	QueryTarget_QUERY_TARGET_TRANSACTIONS: "TRANSACTIONS",
	QueryTarget_QUERY_TARGET_LOGS:         "LOGS",
}

// MarshalJSON implements json.Marshaler for PreparedQueryCursor.
//
// The cursor carries exactly one populated data field per query target:
// accountData (ACCOUNTS), transactionData (TRANSACTIONS) or logData (LOGS).
func (x *PreparedQueryCursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		PageSize        uint32         `json:"pageSize"`
		HasMore         bool           `json:"hasMore"`
		Previous        string         `json:"previous,omitempty"`
		Next            string         `json:"next,omitempty"`
		AccountData     []*Account     `json:"accountData,omitempty"`
		TransactionData []*Transaction `json:"transactionData,omitempty"`
		LogData         []*Log         `json:"logData,omitempty"`
	}{
		PageSize:        x.GetPageSize(),
		HasMore:         x.GetHasMore(),
		Previous:        x.GetPrevious(),
		Next:            x.GetNext(),
		AccountData:     x.GetAccountData(),
		TransactionData: x.GetTransactionData(),
		LogData:         x.GetLogData(),
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
