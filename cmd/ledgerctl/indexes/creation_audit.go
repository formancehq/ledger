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

// filterIndexesByCreationKey reads the primary audit sequence rather than its
// asynchronous filter indexes. A successful singleton creation is attribution,
// not authorization: a caller able to write can also choose the same prefix.
func filterIndexesByCreationKey(ctx context.Context, client servicepb.BucketServiceClient, ledger, prefix string, entries []*commonpb.Index) ([]*commonpb.Index, error) {
	if len(entries) == 0 {
		return []*commonpb.Index{}, nil
	}

	matched := make(map[string]bool)
	current := make(map[string]*commonpb.Index, len(entries))
	for _, entry := range entries {
		current[indexes.Canonical(entry.GetId())] = entry
	}
	cursor := ""
	seen := make(map[string]bool)
	for {
		stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{Cursor: cursor, PageSize: 1000}})
		if err != nil {
			return nil, fmt.Errorf("listing creation audit: %w", err)
		}
		page, err := cmdutil.CollectStream(stream)
		if err != nil {
			return nil, fmt.Errorf("receiving creation audit: %w", err)
		}
		for _, header := range page {
			if !strings.HasPrefix(header.GetIdempotency().GetKey(), prefix) || header.GetSuccess() == nil || header.GetOrderCount() != 1 {
				continue
			}
			full, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{Sequence: header.GetSequence()})
			if err != nil {
				return nil, fmt.Errorf("reading creation audit %d: %w", header.GetSequence(), err)
			}
			if full.GetSequence() != header.GetSequence() {
				return nil, fmt.Errorf("creation audit sequence mismatch: requested %d, received %d", header.GetSequence(), full.GetSequence())
			}
			canonical, err := attributedIndex(full, ledger, prefix, current)
			if err != nil {
				return nil, err
			}
			if canonical != "" {
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

func attributedIndex(entry *auditpb.AuditEntry, ledger, prefix string, current map[string]*commonpb.Index) (string, error) {
	success := entry.GetSuccess()
	if prefix == "" || !strings.HasPrefix(entry.GetIdempotency().GetKey(), prefix) || success == nil || entry.GetOrderCount() != 1 || len(entry.GetItems()) != 1 {
		return "", nil
	}
	item := entry.GetItems()[0]
	seq := item.GetLogSequence()
	if item.GetOrderIndex() != 0 || seq == 0 || success.GetMinLogSequence() != seq || success.GetMaxLogSequence() != seq {
		return "", nil
	}
	order := &raftcmdpb.Order{}
	if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
		return "", fmt.Errorf("decoding creation audit %d: %w", entry.GetSequence(), err)
	}
	scoped := order.GetLedgerScoped()
	create := scoped.GetApply().GetCreateIndex()
	if scoped.GetLedger() != ledger || create == nil || !indexes.Supported(create.GetId()) {
		return "", nil
	}
	canonical := indexes.Canonical(create.GetId())
	idx := current[canonical]
	if idx == nil || entry.GetTimestamp() == nil || idx.GetCreatedAt() == nil || entry.GetTimestamp().GetData() != idx.GetCreatedAt().GetData() {
		return "", nil
	}

	return canonical, nil
}
