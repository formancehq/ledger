package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadLastAppliedProposalSequence returns the sequence of the most recently
// written AppliedProposal entry, or 0 if none exist.
func ReadLastAppliedProposalSequence(reader dal.PebbleReader) (uint64, error) {
	entry, err := ReadLastAppliedProposal(reader)
	if err != nil {
		return 0, err
	}

	if entry == nil {
		return 0, nil
	}

	return entry.GetSequence(), nil
}

// ReadLastAppliedProposal returns the most recently written AppliedProposal
// entry, or nil if none exist.
func ReadLastAppliedProposal(reader dal.PebbleReader) (*proposalpb.AppliedProposal, error) {
	entry, err := dal.ReadLastEntry[*proposalpb.AppliedProposal](reader, dal.ZoneCold, dal.SubColdAppliedProposal)
	if err != nil {
		return nil, fmt.Errorf("reading last applied proposal: %w", err)
	}

	return entry, nil
}

// ReadAppliedProposals returns a cursor over AppliedProposal entries with a
// sequence strictly greater than afterSequence. Pass nil to iterate from the
// beginning. The index builder uses this cursor to learn the per-batch
// transient account exclusion set without touching the audit log.
func ReadAppliedProposals(ctx context.Context, reader dal.PebbleReader, afterSequence *uint64) (cursor.Cursor[*proposalpb.AppliedProposal], error) {
	_, span := queryTracer.Start(ctx, "query.list_applied_proposals")
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal)

	if afterSequence != nil {
		kb.PutUint64(*afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).
		PutBytes(dal.MaxUint64Bytes)
	upperBound := kb2.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for applied proposals: %w", err)
	}

	return dal.NewProtoCursor[*proposalpb.AppliedProposal](iter), nil
}

// ReadAppliedProposal returns the AppliedProposal at the given sequence, or
// domain.ErrNotFound when no entry exists at that sequence (failed proposals
// leave gaps).
func ReadAppliedProposal(ctx context.Context, reader dal.PebbleGetter, sequence uint64) (*proposalpb.AppliedProposal, error) {
	_, span := queryTracer.Start(ctx, "query.get_applied_proposal",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(sequence)

	entry, err := dal.ReadProto[*proposalpb.AppliedProposal](reader, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("reading applied proposal %d: %w", sequence, err)
	}

	if entry == nil {
		return nil, domain.ErrNotFound
	}

	return entry, nil
}
