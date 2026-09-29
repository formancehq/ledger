package indexes

import (
	"context"
	"errors"
	"fmt"
	"strings"

	auditpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// filterIndexesByCreationKey uses the idempotency-key index on the audit
// projection to find creation records whose key shares the operator prefix,
// then applies attributedIndex to confirm each entry really matches the tracked
// registry row. The index eliminates the full audit scan; a prefix operand is
// safe because each attempt appends a unique suffix after the UID segment.
func filterIndexesByCreationKey(ctx context.Context, client auditpb.BucketServiceClient, ledger, prefix string, entries []*auditpb.Index) ([]*auditpb.Index, error) {
	if len(entries) == 0 {
		return []*auditpb.Index{}, nil
	}

	current := make(map[string]*auditpb.Index, len(entries))
	for _, entry := range entries {
		current[indexes.Canonical(entry.GetId())] = entry
	}

	matched := make(map[string]bool)
	cursor := ""
	seen := make(map[string]bool)

	// Build an idempotency-key prefix filter so the server uses the secondary
	// audit index instead of scanning every entry.
	filter := &auditpb.QueryFilter{
		Filter: &auditpb.QueryFilter_Audit{
			Audit: &auditpb.AuditCondition{
				Field:     auditpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
				Condition: &auditpb.AuditCondition_StringPrefix{StringPrefix: prefix},
			},
		},
	}

	for {
		stream, err := client.ListAuditEntries(ctx, &auditpb.ListAuditEntriesRequest{
			Options: &auditpb.ListOptions{
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
			if !strings.HasPrefix(header.GetIdempotency().GetKey(), prefix) || header.GetSuccess() == nil || header.GetOrderCount() != 1 {
				continue
			}
			full, err := client.GetAuditEntry(ctx, &auditpb.GetAuditEntryRequest{Sequence: header.GetSequence()})
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

	result := make([]*auditpb.Index, 0, len(matched))
	for _, entry := range entries {
		if matched[indexes.Canonical(entry.GetId())] {
			result = append(result, entry)
		}
	}

	return result, nil
}

func attributedIndex(entry *auditpb.AuditEntry, ledger, prefix string, current map[string]*auditpb.Index) (string, error) {
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
