package auditindexer

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// emitFn receives one fully-built index key (empty value). Each key slice
// returned by the AuditIndex*Key builders is a fresh allocation from
// KeyBuilder.Build(), so the caller need not copy before retaining.
type emitFn func(key []byte) error

// appendEntryKeys derives every audit-index key for a single AuditEntry and
// passes each to emit. items may be nil (header-only fields are still indexed).
//
// Keys emitted per entry:
//   - AuditFieldOutcome      — 1 key (0=failure, 1=success)
//   - AuditFieldLedger       — 1 key per ledger name
//   - AuditFieldCallerSubject — 1 key when the caller subject is non-empty
//   - AuditFieldTimestamp    — 1 key (raw HLC value from Timestamp.Data, unix microseconds, BE uint64)
//   - AuditFieldProposalID   — 1 key
//   - AuditFieldOrderType    — 1 key per distinct order type across items
//   - AuditFieldLogSeq       — 1 key per item whose LogSequence != 0
func appendEntryKeys(kb *dal.KeyBuilder, emit emitFn, entry *auditpb.AuditEntry, items []*auditpb.AuditItem) error {
	seq := entry.GetSequence()

	// Outcome: 1 = success, 0 = failure.
	var outcome byte
	if _, ok := entry.GetOutcome().(*auditpb.AuditEntry_Success); ok {
		outcome = 1
	}
	if err := emit(readstore.AuditIndexByteKey(kb, readstore.AuditFieldOutcome, outcome, seq)); err != nil {
		return err
	}

	// One key per ledger name.
	for _, ledger := range entry.GetLedgers() {
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, ledger, seq)); err != nil {
			return err
		}
	}

	// Caller subject — skipped when absent (unauthenticated or nil snapshot).
	if subject := entry.GetCallerSnapshot().GetIdentity().GetSubject(); subject != "" {
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldCallerSubject, subject, seq)); err != nil {
			return err
		}
	}

	// Timestamp as HLC microseconds (the scalar carries unix micros; 0 = unset).
	if ts := entry.GetTimestamp(); ts != 0 {
		if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldTimestamp, ts, seq)); err != nil {
			return err
		}
	}

	// Proposal ID.
	if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldProposalID, entry.GetProposalId(), seq)); err != nil {
		return err
	}

	// Per-item fields: log_seq (skip zero) and order_type (dedup per entry).
	seenType := make(map[string]struct{}, len(items))
	for _, item := range items {
		if logSeq := item.GetLogSequence(); logSeq != 0 {
			if err := emit(readstore.AuditIndexUint64Key(kb, readstore.AuditFieldLogSeq, logSeq, seq)); err != nil {
				return err
			}
		}

		raw := item.GetSerializedOrder()
		if len(raw) == 0 {
			continue
		}
		order := &raftcmdpb.Order{}
		if err := proto.Unmarshal(raw, order); err != nil {
			return fmt.Errorf("unmarshaling order for audit seq %d item %d: %w", seq, item.GetOrderIndex(), err)
		}
		token := domain.AuditOrderType(order)
		if _, done := seenType[token]; done {
			continue
		}
		seenType[token] = struct{}{}
		if err := emit(readstore.AuditIndexStringKey(kb, readstore.AuditFieldOrderType, token, seq)); err != nil {
			return err
		}
	}

	return nil
}
