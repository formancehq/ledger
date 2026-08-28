package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runAuditQuery exercises ListAuditEntries with the audit filter leaves. The
// audit trail has no model twin, so the page is validated for
// self-consistency instead: size within the request, sequences strictly
// monotone in the requested order, and every entry inside the filter's scope
// (ledger membership / outcome kind).
func runAuditQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	var (
		filter       *commonpb.QueryFilter
		scopedLedger string
		scopedKind   string
	)

	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		// Unfiltered.
	case 1:
		scopedLedger = random.RandomChoice(c.ledgerNames)
		filter = filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, scopedLedger)
	default:
		scopedKind = random.RandomChoice([]string{"success", "failure"})
		filter = filterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, scopedKind)
	}

	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.observedFrontier()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListAuditEntries(readCtx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Reverse:  reverse,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var entries []*auditEntry
	if err == nil {
		raw, drainErr := drainStream(stream)
		err = drainErr

		for _, e := range raw {
			entries = append(entries, &auditEntry{
				seq:     e.GetSequence(),
				ledgers: e.GetLedgers(),
				failed:  e.GetFailure() != nil,
			})
		}
	}

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListAuditEntries returned unexpected error", internal.Details{
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	violation := auditPageViolation(entries, pageSize, reverse, scopedLedger, scopedKind)
	if violation == "" {
		// Coverage: an audit page answered and held its own contract.
		assert.Reachable("singleton_driver_model: audit page validated", internal.Details{"filter": describeFilter(filter)})

		return
	}

	seqs := make([]string, 0, len(entries))
	for _, e := range entries {
		seqs = append(seqs, strconv.FormatUint(e.seq, 10))
	}

	assert.Unreachable("singleton_driver_model: audit page violates its own contract", internal.Details{
		"violation": violation,
		"filter":    describeFilter(filter),
		"pageSize":  pageSize,
		"reverse":   reverse,
		"rows":      len(entries),
		"seqs":      strings.Join(seqs, ","),
	})
}

type auditEntry struct {
	seq     uint64
	ledgers []string
	failed  bool
}

// auditPageViolation reports the first way the page breaks its own contract,
// or "" when it holds.
func auditPageViolation(entries []*auditEntry, pageSize int, reverse bool, scopedLedger, scopedKind string) string {
	if len(entries) > pageSize {
		return "page longer than requested"
	}

	for i, e := range entries {
		if i > 0 {
			prev := entries[i-1].seq
			if !reverse && e.seq <= prev {
				return "ascending order violated"
			}

			if reverse && e.seq >= prev {
				return "descending order violated"
			}
		}

		if scopedLedger != "" {
			found := false
			for _, l := range e.ledgers {
				if l == scopedLedger {
					found = true

					break
				}
			}

			if !found {
				return "entry outside the ledger scope"
			}
		}

		if scopedKind == "success" && e.failed {
			return "failure entry in a success-scoped page"
		}

		if scopedKind == "failure" && !e.failed {
			return "success entry in a failure-scoped page"
		}
	}

	return ""
}

// filterAuditString builds an audit leaf on a string-typed audit field.
func filterAuditString(field commonpb.AuditField, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field:     field,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value}}},
	}}}
}
