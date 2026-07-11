package auditpb

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	commonpb "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// This file gives the audit protobuf types a hand-written JSON codec so the REST
// surface (GET /v3/_/audit-entries[/{sequence}]) emits camelCase property names,
// matching the rest of the HTTP API (see commonpb.common.pb.json.go). The
// default sonic struct-tag encoding would emit snake_case (proposal_id,
// order_count, …) taken from the generated protobuf struct tags, which violates
// the camelCase JSON invariant.
//
// Timestamps reuse commonpb.Timestamp.MarshalJSON (RFC3339), keeping audit
// timestamps identical to Log/Transaction timestamps on the wire. Sub-messages
// that have no hand-written Go marshaler (CallerSnapshot, Idempotency,
// SignedApplyBatch) and the ErrorReason enum are rendered via protojson, which
// also emits camelCase field names and the enum's string form.

// protoFieldJSON marshals a proto.Message field to json.RawValue using protojson,
// preserving camelCase field names.
//
// A nil message — including a typed-nil pointer (e.g. a *CallerSnapshot that is
// nil), which does NOT compare == nil as an interface and would otherwise
// marshal to "{}" and slip past the omitempty guard — yields (nil, nil): the
// field is simply absent.
//
// A protojson failure is PROPAGATED, never swallowed. These fields
// (callerSnapshot, idempotency, signature) are chain-bound audit evidence; a
// serialization defect must surface as a failed response, not a valid-looking
// record with the field silently dropped (invariant #7: never silently skip a
// "should not happen" branch).
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
func (x *AuditEntry) MarshalJSON() ([]byte, error) {
	type Aux struct {
		Sequence       uint64              `json:"sequence,omitempty"`
		Timestamp      *commonpb.Timestamp `json:"timestamp,omitempty"`
		ProposalId     uint64              `json:"proposalId,omitempty"`
		Success        *AuditSuccess       `json:"success,omitempty"`
		Failure        *AuditFailure       `json:"failure,omitempty"`
		OrderCount     uint32              `json:"orderCount,omitempty"`
		Items          []*AuditItem        `json:"items,omitempty"`
		Ledgers        []string            `json:"ledgers,omitempty"`
		Hash           string              `json:"hash,omitempty"`
		HashVersion    uint32              `json:"hashVersion,omitempty"`
		CallerSnapshot json.RawValue       `json:"callerSnapshot,omitempty"`
		Idempotency    json.RawValue       `json:"idempotency,omitempty"`
		Signature      json.RawValue       `json:"signature,omitempty"`
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
func (x *AuditItem) MarshalJSON() ([]byte, error) {
	type Aux struct {
		OrderIndex      uint32 `json:"orderIndex,omitempty"`
		SerializedOrder string `json:"serializedOrder,omitempty"`
		LogSequence     uint64 `json:"logSequence,omitempty"`
	}

	aux := Aux{
		OrderIndex:  x.GetOrderIndex(),
		LogSequence: x.GetLogSequence(),
	}

	if so := x.GetSerializedOrder(); len(so) > 0 {
		aux.SerializedOrder = hex.EncodeToString(so)
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for AuditSuccess.
func (x *AuditSuccess) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		MinLogSequence uint64 `json:"minLogSequence,omitempty"`
		MaxLogSequence uint64 `json:"maxLogSequence,omitempty"`
	}{
		MinLogSequence: x.GetMinLogSequence(),
		MaxLogSequence: x.GetMaxLogSequence(),
	})
}

// MarshalJSON implements json.Marshaler for AuditFailure. The reason enum is
// rendered as its protobuf string name (e.g. "ERROR_REASON_INSUFFICIENT_FUNDS")
// so REST consumers see a stable, self-describing identifier rather than an
// opaque integer.
func (x *AuditFailure) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Reason  string            `json:"reason,omitempty"`
		Message string            `json:"message,omitempty"`
		Context map[string]string `json:"context,omitempty"`
	}{
		Reason:  x.GetReason().String(),
		Message: x.GetMessage(),
		Context: x.GetContext(),
	})
}
