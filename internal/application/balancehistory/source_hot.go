package balancehistory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// HotSource reads the audit source still present in the primary Pebble store.
// Every operation owns one bounded-lifetime snapshot, which keeps each batch
// internally consistent without pinning primary-store files across builder
// iterations.
type HotSource struct {
	reader dal.SnapshotReader
}

func NewHotSource(reader dal.SnapshotReader) *HotSource {
	return &HotSource{reader: reader}
}

func (s *HotSource) Head(ctx context.Context) (position Position, err error) {
	if err := ctx.Err(); err != nil {
		return Position{}, err
	}

	handle, err := s.reader.NewReadHandle()
	if err != nil {
		return Position{}, fmt.Errorf("opening hot balance history source snapshot: %w", err)
	}
	defer func() {
		if closeErr := handle.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing hot balance history source snapshot: %w", closeErr))
		}
	}()

	return readHotHead(handle)
}

func (s *HotSource) Read(
	ctx context.Context,
	after Position,
	maxProposals int,
) (batch Batch, err error) {
	if maxProposals <= 0 {
		return Batch{}, fmt.Errorf("balance history source batch limit must be positive, got %d", maxProposals)
	}
	if err := ctx.Err(); err != nil {
		return Batch{}, err
	}

	handle, err := s.reader.NewReadHandle()
	if err != nil {
		return Batch{}, fmt.Errorf("opening hot balance history source snapshot: %w", err)
	}
	defer func() {
		if closeErr := handle.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing hot balance history source snapshot: %w", closeErr))
		}
	}()

	head, err := readHotHead(handle)
	if err != nil {
		return Batch{}, err
	}
	if after.AuditSequence > head.AuditSequence {
		return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
			"cursor audit sequence %d is ahead of hot head %d",
			after.AuditSequence,
			head.AuditSequence,
		)}
	}
	if after.LogSequence > head.LogSequence {
		return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
			"cursor log sequence %d is ahead of hot head %d",
			after.LogSequence,
			head.LogSequence,
		)}
	}
	if after.AuditSequence == head.AuditSequence {
		if after.LogSequence != head.LogSequence {
			return Batch{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"cursor covers audit head %d but log watermark is %d instead of %d",
				head.AuditSequence,
				after.LogSequence,
				head.LogSequence,
			)}
		}
		if len(after.AuditHash) > 0 && len(head.AuditHash) > 0 && !bytes.Equal(after.AuditHash, head.AuditHash) {
			return Batch{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"cursor audit hash %x differs from hot head hash %x at sequence %d",
				after.AuditHash,
				head.AuditHash,
				head.AuditSequence,
			)}
		}

		return Batch{Next: after, Head: head}, nil
	}

	afterAudit := after.AuditSequence
	entries, err := query.ReadAuditEntries(ctx, handle, &afterAudit)
	if err != nil {
		return Batch{}, fmt.Errorf("opening hot audit entries after %d: %w", after.AuditSequence, err)
	}
	defer func() {
		if closeErr := entries.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing hot audit entry cursor: %w", closeErr))
		}
	}()

	proposals := make([]VerifiedProposal, 0, maxProposals)
	next := Position{
		AuditSequence: after.AuditSequence,
		LogSequence:   after.LogSequence,
		AuditHash:     append([]byte(nil), after.AuditHash...),
	}
	expectedAuditSequence := after.AuditSequence + 1

	for len(proposals) < maxProposals {
		if err := ctx.Err(); err != nil {
			return Batch{}, err
		}

		entry, nextErr := entries.Next()
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				break
			}

			return Batch{}, fmt.Errorf("reading hot audit entry after %d: %w", next.AuditSequence, nextErr)
		}
		if entry.GetSequence() != expectedAuditSequence {
			return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
				"expected audit sequence %d, first available sequence is %d",
				expectedAuditSequence,
				entry.GetSequence(),
			)}
		}

		proposal, maxLogSequence, verifyErr := readProposalHeader(ctx, handle, entry)
		if verifyErr != nil {
			return Batch{}, verifyErr
		}
		if maxLogSequence > 0 {
			minLogSequence := proposal.Entry.GetSuccess().GetMinLogSequence()
			if minLogSequence != next.LogSequence+1 {
				return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
					"audit sequence %d starts fresh log range at %d after watermark %d",
					entry.GetSequence(),
					minLogSequence,
					next.LogSequence,
				)}
			}
		}
		proposals = append(proposals, proposal)

		next.AuditSequence = entry.GetSequence()
		if maxLogSequence > 0 {
			next.LogSequence = maxLogSequence
		}
		next.AuditHash = append([]byte(nil), entry.GetHash()...)
		expectedAuditSequence++
	}

	if len(proposals) == 0 && next.AuditSequence < head.AuditSequence {
		return Batch{}, &ErrSourceMissing{Detail: fmt.Sprintf(
			"expected audit sequence %d, but no hot audit entry is available before head %d",
			expectedAuditSequence,
			head.AuditSequence,
		)}
	}
	if err := resolveProposalLogsFromReader(ctx, handle, proposals); err != nil {
		return Batch{}, fmt.Errorf("resolving hot source logs: %w", err)
	}

	return Batch{Proposals: proposals, Next: next, Head: head}, nil
}

func readHotHead(reader dal.PebbleReader) (Position, error) {
	entry, err := query.ReadLastAuditEntry(reader)
	if err != nil {
		return Position{}, fmt.Errorf("reading hot audit head: %w", err)
	}

	logSequence, err := query.ReadLastSequence(reader)
	if err != nil {
		return Position{}, fmt.Errorf("reading hot log head: %w", err)
	}

	position := Position{LogSequence: logSequence}
	if entry != nil {
		position.AuditSequence = entry.GetSequence()
		position.AuditHash = append([]byte(nil), entry.GetHash()...)
	}

	return position, nil
}
