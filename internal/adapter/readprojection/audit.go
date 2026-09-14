package readprojection

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Audit returns a display projection for public reads. Persisted audit records
// remain the authoritative hash/signature preimages. When credentials change,
// the returned bytes cannot be used to verify the original hash or signature.
// Unchanged orders and signed batches retain their exact original encoding.
func Audit(entry *auditpb.AuditEntry) (*auditpb.AuditEntry, error) {
	if entry == nil {
		return nil, nil
	}
	result := entry.CloneVT()
	for _, item := range result.GetItems() {
		if item == nil || len(item.GetSerializedOrder()) == 0 {
			continue
		}
		order := &raftcmdpb.Order{}
		if err := validateCredentialWire(item.GetSerializedOrder(), order); err != nil {
			return nil, fmt.Errorf("decoding audit order for public projection: %w", err)
		}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil, fmt.Errorf("decoding audit order for public projection: %w", err)
		}
		if redactOrder(order) {
			serialized, err := order.MarshalVT()
			if err != nil {
				return nil, fmt.Errorf("encoding audit order for public projection: %w", err)
			}
			item.SerializedOrder = serialized
		}
	}
	if signed := result.GetSignature(); signed != nil && len(signed.GetPayload()) > 0 {
		batch := &servicepb.ApplyBatch{}
		if err := validateCredentialWire(signed.GetPayload(), batch); err != nil {
			return nil, fmt.Errorf("decoding signed audit batch for public projection: %w", err)
		}
		if err := batch.UnmarshalVT(signed.GetPayload()); err != nil {
			return nil, fmt.Errorf("decoding signed audit batch for public projection: %w", err)
		}
		changed := false
		for _, request := range batch.GetRequests() {
			changed = redactSink(request.GetAddEventsSink().GetConfig()) || changed
			changed = redactMirror(request.GetCreateLedger().GetMirrorSource()) || changed
		}
		if changed {
			payload, err := batch.MarshalVT()
			if err != nil {
				return nil, fmt.Errorf("encoding signed audit batch for public projection: %w", err)
			}
			signed.Payload = payload
			// Keep the signing key identity, but do not present the old signature
			// as proof of the redacted payload. The stored envelope is unchanged.
			signed.Signature = nil
		}
	}

	return result, nil
}

func redactOrder(order *raftcmdpb.Order) bool {
	changed := redactSink(order.GetSystemScoped().GetAddEventsSink().GetConfig())

	return redactMirror(order.GetLedgerScoped().GetCreateLedger().GetMirrorSource()) || changed
}

// Log returns a deep-cloned system-log display projection. It must never be
// used by replay, backup, the checker, or delivery workers, which need the
// original creation-time configuration. Response-signature envelopes normally
// occur only on write responses; if present on a read, their payload is also
// projected and their signature is retained only when its bytes are unchanged.
func Log(entry *commonpb.Log) (*commonpb.Log, error) {
	if entry == nil {
		return nil, nil
	}
	result := entry.CloneVT()
	redactLogPayload(result)
	if signed := result.GetResponseSignature(); signed != nil && len(signed.GetPayload()) > 0 {
		payload := &commonpb.Log{}
		if err := validateCredentialWire(signed.GetPayload(), payload); err != nil {
			return nil, fmt.Errorf("decoding signed log for public projection: %w", err)
		}
		if err := payload.UnmarshalVT(signed.GetPayload()); err != nil {
			return nil, fmt.Errorf("decoding signed log for public projection: %w", err)
		}
		// SignLog excludes the envelope itself from its signed preimage. A
		// nested envelope violates that contract; never send unchecked bytes.
		if payload.GetResponseSignature() != nil {
			return nil, errors.New("nested response signature in public log projection")
		}
		if redactLogPayload(payload) {
			serialized, err := payload.MarshalVT()
			if err != nil {
				return nil, fmt.Errorf("encoding signed log for public projection: %w", err)
			}
			signed.Payload = serialized
			signed.Signature = nil
		}
	}

	return result, nil
}

func redactLogPayload(entry *commonpb.Log) bool {
	changed := redactSink(entry.GetPayload().GetAddedEventsSink().GetConfig())

	return redactMirror(entry.GetPayload().GetCreateLedger().GetMirrorSource()) || changed
}
