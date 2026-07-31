package balancehistory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

		proposal, maxLogSequence, verifyErr := readVerifiedProposal(ctx, handle, entry)
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

func readVerifiedProposal(
	ctx context.Context,
	reader dal.PebbleReader,
	entry *auditpb.AuditEntry,
) (VerifiedProposal, uint64, error) {
	auditSequence := entry.GetSequence()
	if len(entry.GetItems()) != 0 {
		return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d embeds %d items in its header",
			auditSequence,
			len(entry.GetItems()),
		)}
	}
	if entry.GetSuccess() == nil && entry.GetFailure() == nil {
		return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d has no outcome",
			auditSequence,
		)}
	}

	items, err := query.ReadAuditItems(ctx, reader, auditSequence)
	if err != nil {
		return VerifiedProposal{}, 0, fmt.Errorf("reading audit items for sequence %d: %w", auditSequence, err)
	}
	if uint32(len(items)) != entry.GetOrderCount() {
		return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
			"audit sequence %d declares %d items but %d are available",
			auditSequence,
			entry.GetOrderCount(),
			len(items),
		)}
	}

	logs := make([]*commonpb.Log, len(items))
	var minLogSequence, maxLogSequence uint64
	if success := entry.GetSuccess(); success != nil {
		minLogSequence = success.GetMinLogSequence()
		maxLogSequence = success.GetMaxLogSequence()
		if (minLogSequence == 0) != (maxLogSequence == 0) {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d has partial fresh log range [%d,%d]",
				auditSequence,
				minLogSequence,
				maxLogSequence,
			)}
		}
		if minLogSequence > maxLogSequence {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d has descending fresh log range [%d,%d]",
				auditSequence,
				minLogSequence,
				maxLogSequence,
			)}
		}
		if minLogSequence > 0 && maxLogSequence-minLogSequence+1 > uint64(len(items)) {
			return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d fresh log range [%d,%d] cannot fit in %d items",
				auditSequence,
				minLogSequence,
				maxLogSequence,
				len(items),
			)}
		}
	}
	freshLogs := make(map[uint64]struct{})

	for index, item := range items {
		if item == nil {
			return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d is missing item %d",
				auditSequence,
				index,
			)}
		}
		if item.GetOrderIndex() != uint32(index) {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d item at position %d declares order index %d",
				auditSequence,
				index,
				item.GetOrderIndex(),
			)}
		}

		logSequence := item.GetLogSequence()
		if logSequence == 0 {
			continue
		}
		if entry.GetFailure() != nil {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"failed audit sequence %d item %d references log %d",
				auditSequence,
				index,
				logSequence,
			)}
		}
		if logSequence > maxLogSequence {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d item %d references log %d beyond fresh range maximum %d",
				auditSequence,
				index,
				logSequence,
				maxLogSequence,
			)}
		}
		if minLogSequence > 0 && logSequence >= minLogSequence {
			if _, exists := freshLogs[logSequence]; exists {
				return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d references fresh log %d more than once",
					auditSequence,
					logSequence,
				)}
			}
			freshLogs[logSequence] = struct{}{}
		}

		log, err := query.ReadLogBySequence(ctx, reader, logSequence)
		if err != nil {
			return VerifiedProposal{}, 0, fmt.Errorf(
				"reading log %d referenced by audit sequence %d item %d: %w",
				logSequence,
				auditSequence,
				index,
				err,
			)
		}
		if log == nil {
			return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d item %d references missing log %d",
				auditSequence,
				index,
				logSequence,
			)}
		}
		if log.GetSequence() != logSequence {
			return VerifiedProposal{}, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit sequence %d item %d references log %d whose payload sequence is %d",
				auditSequence,
				index,
				logSequence,
				log.GetSequence(),
			)}
		}

		logs[index] = log
	}

	if minLogSequence > 0 {
		expectedFreshLogs := maxLogSequence - minLogSequence + 1
		if uint64(len(freshLogs)) != expectedFreshLogs {
			for offset := range expectedFreshLogs {
				sequence := minLogSequence + offset
				if _, exists := freshLogs[sequence]; !exists {
					return VerifiedProposal{}, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
						"audit sequence %d is missing fresh log %d from range [%d,%d]",
						auditSequence,
						sequence,
						minLogSequence,
						maxLogSequence,
					)}
				}
			}
		}
	}

	return VerifiedProposal{Entry: entry, Items: items, Logs: logs}, maxLogSequence, nil
}
