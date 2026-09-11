package publicauditpb

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Public audit JSON preserves numeric sequence fields and RFC3339 timestamps.
// Order details are typed messages, never hex or base64 evidence containers.
func protoFieldJSON(msg proto.Message) (json.RawValue, error) {
	if msg == nil {
		return nil, nil
	}

	if v := reflect.ValueOf(msg); v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, nil
	}

	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// MarshalJSON implements json.Marshaler for AuditEntry.
//
// Scalar identity/counter fields (sequence, proposalId, orderCount,
// hashVersion) are serialized unconditionally: they are present on every
// entry and their zero value is a real value (e.g. the first audit entry can
// carry sequence 0), so omitempty would silently drop a documented field and
// break generated clients. omitempty is kept only for fields that are
// legitimately absent — the success/failure oneof, timestamp, the optional
// submessages, and the empty-on-list slices.
func (x *AuditEntry) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Sequence       uint64                `json:"sequence"`
		Timestamp      *commonpb.Timestamp   `json:"timestamp,omitempty"`
		ProposalId     uint64                `json:"proposalId"`
		Success        *auditpb.AuditSuccess `json:"success,omitempty"`
		Failure        *AuditFailure         `json:"failure,omitempty"`
		OrderCount     uint32                `json:"orderCount"`
		Items          []*AuditItem          `json:"items,omitempty"`
		Ledgers        []string              `json:"ledgers,omitempty"`
		Hash           string                `json:"hash,omitempty"`
		HashVersion    uint32                `json:"hashVersion"`
		CallerSnapshot json.RawValue         `json:"callerSnapshot,omitempty"`
		Idempotency    json.RawValue         `json:"idempotency,omitempty"`
		Signature      json.RawValue         `json:"signature,omitempty"`
	}

	aux := Aux{
		Sequence:    x.GetSequence(),
		Timestamp:   x.GetTimestamp(),
		ProposalId:  x.GetProposalId(),
		Success:     x.GetSuccess(),
		Failure:     x.GetFailure(),
		OrderCount:  x.GetOrderCount(),
		Items:       x.GetItems(),
		Ledgers:     x.GetLedgers(),
		HashVersion: x.GetHashVersion(),
	}

	var err error

	if aux.CallerSnapshot, err = protoFieldJSON(x.GetCallerSnapshot()); err != nil {
		return nil, fmt.Errorf("audit entry %d: marshaling callerSnapshot: %w", x.GetSequence(), err)
	}

	if aux.Idempotency, err = protoFieldJSON(x.GetIdempotency()); err != nil {
		return nil, fmt.Errorf("audit entry %d: marshaling idempotency: %w", x.GetSequence(), err)
	}

	if aux.Signature, err = protoFieldJSON(x.GetSignature()); err != nil {
		return nil, fmt.Errorf("audit entry %d: marshaling signature: %w", x.GetSequence(), err)
	}

	if h := x.GetHash(); len(h) > 0 {
		aux.Hash = hex.EncodeToString(h)
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for AuditItem.
//
// orderIndex and logSequence carry meaning at zero, so they are serialized
// unconditionally: orderIndex is zero-based (the first item of every entry is
// 0) and logSequence is documented as the 0 sentinel for an idempotent replay
// or a failed proposal. omitempty would drop these, making it impossible for a
// client to locate the first order or interpret the no-log sentinel.
func (x *AuditItem) MarshalJSON() ([]byte, error) {
	order, err := protoFieldJSON(x.GetOrder())
	if err != nil {
		return nil, fmt.Errorf("audit item %d: marshaling order: %w", x.GetOrderIndex(), err)
	}

	return json.Marshal(&struct {
		OrderIndex  uint32        `json:"orderIndex"`
		LogSequence uint64        `json:"logSequence"`
		Order       json.RawValue `json:"order,omitempty"`
	}{OrderIndex: x.GetOrderIndex(), LogSequence: x.GetLogSequence(), Order: order})
}

// MarshalJSON retains the stable textual error reason in public diagnostics.
func (x *AuditFailure) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Reason  string            `json:"reason,omitempty"`
		Message string            `json:"message,omitempty"`
		Context map[string]string `json:"context,omitempty"`
	}{Reason: x.GetReason().String(), Message: x.GetMessage(), Context: x.GetContext()})
}
