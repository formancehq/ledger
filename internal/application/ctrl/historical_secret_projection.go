package ctrl

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// historicalCursor projects each result without changing the stored record or
// the cursor's ownership of its read snapshot.
type historicalCursor[T any] struct {
	inner   cursor.Cursor[T]
	project func(T) (T, error)
}

func (c *historicalCursor[T]) Next() (T, error) {
	value, err := c.inner.Next()
	if err != nil {
		var zero T

		return zero, err
	}

	return c.project(value)
}

func (c *historicalCursor[T]) Close() error { return c.inner.Close() }

func (c *historicalCursor[T]) HasMore() bool { return cursor.SourceHasMore(c.inner) }

func projectHistoricalLog(log *commonpb.Log) (*commonpb.Log, error) {
	if log == nil {
		return nil, nil
	}
	projected := sensitive.Redact(log)
	if signed := projected.GetResponseSignature(); signed != nil && len(signed.GetPayload()) > 0 {
		original := &commonpb.Log{}
		if err := proto.Unmarshal(signed.GetPayload(), original); err != nil {
			return nil, fmt.Errorf("decoding signed historical log: %w", err)
		}
		redacted := sensitive.Redact(original)
		// A signed payload is expected to be the unsigned log. Drop a nested
		// envelope rather than exposing opaque bytes from malformed history.
		redacted.ResponseSignature = nil
		if !proto.Equal(original, redacted) {
			encoded, err := proto.Marshal(redacted)
			if err != nil {
				return nil, fmt.Errorf("encoding signed historical log: %w", err)
			}
			signed.Payload = encoded
			signed.Signature = nil
		}
	}

	return projected, nil
}

func projectHistoricalAudit(entry *auditpb.AuditEntry) (*auditpb.AuditEntry, error) {
	if entry == nil {
		return nil, nil
	}
	projected := sensitive.Redact(entry)
	for _, item := range projected.GetItems() {
		if len(item.GetSerializedOrder()) == 0 {
			continue
		}
		order := &raftcmdpb.Order{}
		if err := proto.Unmarshal(item.GetSerializedOrder(), order); err != nil {
			return nil, fmt.Errorf("decoding audit order %d: %w", item.GetOrderIndex(), err)
		}
		redacted := sensitive.Redact(order)
		if proto.Equal(order, redacted) {
			continue
		}
		encoded, err := proto.Marshal(redacted)
		if err != nil {
			return nil, fmt.Errorf("encoding audit order %d: %w", item.GetOrderIndex(), err)
		}
		item.SerializedOrder = encoded
	}
	if signature := projected.GetSignature(); signature != nil && len(signature.GetPayload()) > 0 {
		batch := &servicepb.ApplyBatch{}
		if err := proto.Unmarshal(signature.GetPayload(), batch); err != nil {
			return nil, fmt.Errorf("decoding signed audit batch: %w", err)
		}
		redacted := sensitive.Redact(batch)
		if !proto.Equal(batch, redacted) {
			encoded, err := proto.Marshal(redacted)
			if err != nil {
				return nil, fmt.Errorf("encoding signed audit batch: %w", err)
			}
			signature.Payload = encoded
			signature.Signature = nil // The original signature covers the original payload.
		}
	}

	return projected, nil
}
