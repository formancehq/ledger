package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// preparedQueryTargets maps the public REST target enum to the proto
// QueryTarget. Only ACCOUNTS and TRANSACTIONS are exposed over REST: the
// prepared-query execution path (query.Execute) hydrates only account and
// transaction data — there is no log result field on PreparedQueryCursor — so a
// LOGS-target prepared query would execute to an empty cursor. LOGS remains a
// valid direct ListLogs target; it is just not a usable *prepared-query* target
// yet, so REST does not advertise it. See openapi.yml (ACCOUNTS/TRANSACTIONS).
var preparedQueryTargets = map[string]commonpb.QueryTarget{
	"ACCOUNTS":     commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	"TRANSACTIONS": commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
}

// parsePreparedQueryTarget resolves the `target` field of a prepared-query
// request. An unknown or unsupported value is rejected loudly rather than
// silently coerced to a default (which would run a different query than the
// caller asked for).
func parsePreparedQueryTarget(target string) (commonpb.QueryTarget, error) {
	if target == "" {
		return 0, errors.New("target is required (ACCOUNTS or TRANSACTIONS)")
	}

	t, ok := preparedQueryTargets[target]
	if !ok {
		return 0, fmt.Errorf("unknown or unsupported target %q (use ACCOUNTS or TRANSACTIONS)", target)
	}

	return t, nil
}

// decodePreparedQueryFilter decodes the `filter` JSON value of a prepared-query
// create/update request. The value uses the v2-aligned query DSL documented in
// openapi.yml (combinators `$and`/`$or`/`$not` plus single-key operators
// `$match`/`$gt`/`$exists`/…); the codec lives on commonpb.QueryFilter
// (query_filter.go) and keeps the protobuf-internal oneof/wrapper names off the
// public surface. An empty or absent filter is rejected — otherwise the prepared
// query would store nil and fail later at execute time with "unknown filter
// type: <nil>".
func decodePreparedQueryFilter(raw json.RawMessage) (*commonpb.QueryFilter, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, errors.New("filter is required")
	}

	filter := &commonpb.QueryFilter{}
	if err := json.Unmarshal(raw, filter); err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	if filter.GetFilter() == nil {
		return nil, errors.New("filter must contain at least one condition")
	}

	// REST prepared queries only target ACCOUNTS/TRANSACTIONS
	// (see parsePreparedQueryTarget), so log-only conditions (logId, date,
	// ledger) can never be satisfied here — they would compile a log iterator
	// and silently return unrelated/empty results. Reject them up front.
	if err := rejectLogOnlyConditions(filter); err != nil {
		return nil, err
	}

	return filter, nil
}

// rejectLogOnlyConditions walks the filter tree and rejects conditions that only
// make sense for log queries: logId, log date (logBuiltinUint), and ledger. The
// REST prepared-query surface cannot target logs, so these are always invalid.
func rejectLogOnlyConditions(f *commonpb.QueryFilter) error {
	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_LogId:
		return errors.New("logId filter is only valid on log queries, which prepared queries cannot target over REST")
	case *commonpb.QueryFilter_LogBuiltinUint:
		return errors.New("date filter is only valid on log queries, which prepared queries cannot target over REST")
	case *commonpb.QueryFilter_Ledger:
		return errors.New("ledger filter is only valid on log queries, which prepared queries cannot target over REST")
	case *commonpb.QueryFilter_And:
		for _, sub := range v.And.GetFilters() {
			if err := rejectLogOnlyConditions(sub); err != nil {
				return err
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, sub := range v.Or.GetFilters() {
			if err := rejectLogOnlyConditions(sub); err != nil {
				return err
			}
		}
	case *commonpb.QueryFilter_Not:
		return rejectLogOnlyConditions(v.Not.GetFilter())
	}

	return nil
}
