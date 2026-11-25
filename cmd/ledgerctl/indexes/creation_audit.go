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
	return filterIndexesByAuditKey(ctx, client, ledger, prefix, entries, false)
}

func filterInitialIndexesByCreationKey(ctx context.Context, client servicepb.BucketServiceClient, ledger, prefix string, entries []*commonpb.Index) ([]*commonpb.Index, error) {
	return filterIndexesByAuditKey(ctx, client, ledger, prefix, entries, true)
}

func filterIndexesByAuditKey(ctx context.Context, client servicepb.BucketServiceClient, ledger, prefix string, entries []*commonpb.Index, initial bool) ([]*commonpb.Index, error) {
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
			// the entry is a successful singleton, before fetching the full body.
			if !strings.HasPrefix(header.GetIdempotency().GetKey(), prefix) || header.GetSuccess() == nil || (!initial && header.GetOrderCount() != 1) {
				continue
			}
			full, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{Sequence: header.GetSequence()})
			if err != nil {
				return nil, fmt.Errorf("reading creation audit %d: %w", header.GetSequence(), err)
			}
			if full.GetSequence() != header.GetSequence() {
				return nil, fmt.Errorf("creation audit sequence mismatch: requested %d, received %d", header.GetSequence(), full.GetSequence())
			}
			if initial {
				canonicals, err := attributedInitialIndexes(full, ledger, prefix, current)
				if err != nil {
					return nil, err
				}
				for _, canonical := range canonicals {
					matched[canonical] = true
				}
			} else {
				canonical, err := attributedIndex(full, ledger, prefix, current)
				if err != nil {
					return nil, err
				}
				if canonical != "" {
					matched[canonical] = true
				}
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

// attributedInitialIndexes accepts only a successful ledger-create proposal
// followed by fresh index creates for the same ledger. The CR UID in the key
// scopes evidence; current creation dates reject later replacements.
func attributedInitialIndexes(entry *auditpb.AuditEntry, ledger, prefix string, current map[string]*commonpb.Index) ([]string, error) {
	success := entry.GetSuccess()
	items := entry.GetItems()
	if prefix == "" || !strings.HasPrefix(entry.GetIdempotency().GetKey(), prefix) || success == nil || entry.GetOrderCount() < 2 || len(items) != int(entry.GetOrderCount()) || entry.GetTimestamp() == nil {
		return nil, nil
	}
	if success.GetMinLogSequence() == 0 || success.GetMaxLogSequence() < success.GetMinLogSequence() {
		return nil, nil
	}
	var matched []string
	seen := make(map[string]bool)
	previous := uint64(0)
	for i, item := range items {
		if item.GetOrderIndex() != uint32(i) || item.GetLogSequence() == 0 || item.GetLogSequence() < success.GetMinLogSequence() || item.GetLogSequence() > success.GetMaxLogSequence() {
			return nil, nil
		}
		if i == 0 && item.GetLogSequence() != success.GetMinLogSequence() {
			return nil, nil
		}
		if i > 0 && item.GetLogSequence() != previous+1 {
			return nil, nil
		}
		previous = item.GetLogSequence()
		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil, fmt.Errorf("decoding creation audit %d: %w", entry.GetSequence(), err)
		}
		scoped := order.GetLedgerScoped()
		if scoped.GetLedger() != ledger {
			return nil, nil
		}
		if i == 0 {
			if scoped.GetCreateLedger() == nil {
				return nil, nil
			}

			continue
		}
		create := scoped.GetApply().GetCreateIndex()
		if create == nil || !indexes.Supported(create.GetId()) {
			return nil, nil
		}
		canonical := indexes.Canonical(create.GetId())
		if seen[canonical] {
			return nil, nil
		}
		seen[canonical] = true
		idx := current[canonical]
		if idx != nil && idx.GetCreatedAt() != nil && idx.GetCreatedAt().GetData() == entry.GetTimestamp().GetData() {
			matched = append(matched, canonical)
		}
	}
	if previous != success.GetMaxLogSequence() {
		return nil, nil
	}

	return matched, nil
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
