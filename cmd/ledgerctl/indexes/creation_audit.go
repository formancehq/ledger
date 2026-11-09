package indexes

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// filterIndexesByCreationKey uses the idempotency-key index on the audit
// projection to find creation records whose key shares the operator prefix,
// then applies attributedIndex to confirm each entry really matches the tracked
// registry row. The index eliminates the full audit scan; a prefix operand is
// safe because each attempt appends a unique suffix after the UID segment.
func filterIndexesByCreationKey(ctx context.Context, client servicepb.BucketServiceClient, ledger, prefix string, entries []*commonpb.Index) ([]*commonpb.Index, error) {
	if len(entries) == 0 {
		return []*commonpb.Index{}, nil
	}

	current := make(map[string]*commonpb.Index, len(entries))
	for _, entry := range entries {
		current[indexes.Canonical(entry.GetId())] = entry
	}

	matched := make(map[string]bool)
	cursor := ""
	seen := make(map[string]bool)

	// Build an idempotency-key prefix filter so the server uses the secondary
	// audit index instead of scanning every entry.
	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field:     commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
				Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: prefix},
			},
		},
	}

	for {
		stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
			Options: &commonpb.ListOptions{
				Cursor:   cursor,
				PageSize: 1000,
				Filter:   filter,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("listing creation audit: %w", err)
		}
		page, err := cmdutil.CollectStream(stream)
		if err != nil {
			return nil, fmt.Errorf("receiving creation audit: %w", err)
		}
		for _, header := range page {
			// The index pre-filters by prefix; still verify the key matches and
			// the entry succeeded, before fetching the full body.
			if !strings.HasPrefix(header.GetIdempotency().GetKey(), prefix) || header.GetSuccess() == nil || header.GetOrderCount() == 0 {
				continue
			}
			full, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{Sequence: header.GetSequence()})
			if err != nil {
				return nil, fmt.Errorf("reading creation audit %d: %w", header.GetSequence(), err)
			}
			if full.GetSequence() != header.GetSequence() {
				return nil, fmt.Errorf("creation audit sequence mismatch: requested %d, received %d", header.GetSequence(), full.GetSequence())
			}
			attributed, err := attributedIndexes(full, ledger, prefix, current)
			if err != nil {
				return nil, err
			}
			for _, canonical := range attributed {
				matched[canonical] = true
			}
		}
		next := cmdutil.NextCursorFromTrailer(stream.Trailer())
		if next == "" {
			break
		}
		if seen[next] {
			return nil, errors.New("creation audit pagination repeated cursor")
		}
		seen[next] = true
		cursor = next
	}

	result := make([]*commonpb.Index, 0, len(matched))
	for _, entry := range entries {
		if matched[indexes.Canonical(entry.GetId())] {
			result = append(result, entry)
		}
	}

	return result, nil
}

// attributedIndexes returns the indexes a successful, freshly applied audit
// entry under prefix created on ledger and that are still the current ones:
// each CreateIndex order of the entry whose index is listed with the entry's
// timestamp as its creation date, so an index dropped and recreated since is
// not attributed. The entry may be a single CreateIndex or a batch, such as a
// ledger creation that carries its initial indexes (EN-2070); its other
// orders are ignored. A malformed entry attributes nothing: one item per
// order, at distinct positions, each with a distinct log sequence within the
// logged range and together spanning it (a replay logs nothing).
func attributedIndexes(entry *auditpb.AuditEntry, ledger, prefix string, current map[string]*commonpb.Index) ([]string, error) {
	success := entry.GetSuccess()
	items := entry.GetItems()
	if prefix == "" || !strings.HasPrefix(entry.GetIdempotency().GetKey(), prefix) || success == nil || entry.GetTimestamp() == nil ||
		len(items) == 0 || uint64(len(items)) != uint64(entry.GetOrderCount()) || !freshlyApplied(items, success) {
		return nil, nil
	}

	// Orders of one entry share its date, so the date only identifies an
	// index the entry created exactly once and never dropped.
	created := map[string]int{}
	dropped := map[string]bool{}
	var order []string
	for _, item := range items {
		decoded := &raftcmdpb.Order{}
		if err := decoded.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil, fmt.Errorf("decoding creation audit %d: %w", entry.GetSequence(), err)
		}
		scoped := decoded.GetLedgerScoped()
		if scoped.GetLedger() != ledger {
			continue
		}
		if drop := scoped.GetApply().GetDropIndex(); drop != nil {
			dropped[indexes.Canonical(drop.GetId())] = true
		}
		create := scoped.GetApply().GetCreateIndex()
		if create == nil || !indexes.Supported(create.GetId()) {
			continue
		}
		canonical := indexes.Canonical(create.GetId())
		if created[canonical] == 0 {
			order = append(order, canonical)
		}
		created[canonical]++
	}

	var attributed []string
	for _, canonical := range order {
		idx := current[canonical]
		if created[canonical] != 1 || dropped[canonical] || idx == nil || idx.GetCreatedAt() == nil ||
			entry.GetTimestamp().GetData() != idx.GetCreatedAt().GetData() {
			continue
		}
		attributed = append(attributed, canonical)
	}

	return attributed, nil
}

// freshlyApplied reports whether items occupy distinct order positions within
// the entry and carry distinct, non-zero log sequences whose range is exactly
// the one the entry logged.
func freshlyApplied(items []*auditpb.AuditItem, success *auditpb.AuditSuccess) bool {
	positions := make(map[uint32]bool, len(items))
	sequences := make(map[uint64]bool, len(items))
	low, high := uint64(0), uint64(0)
	for _, item := range items {
		position, seq := item.GetOrderIndex(), item.GetLogSequence()
		if uint64(position) >= uint64(len(items)) || positions[position] || seq == 0 || sequences[seq] {
			return false
		}
		positions[position], sequences[seq] = true, true
		if low == 0 || seq < low {
			low = seq
		}
		high = max(high, seq)
	}

	// Each order logs once, so a fresh entry fills its contiguous range.
	return success.GetMinLogSequence() == low && success.GetMaxLogSequence() == high && high-low+1 == uint64(len(items))
}
