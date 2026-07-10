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
	// (see parsePreparedQueryTarget), so a condition that is invalid on BOTH of
	// those targets (logId, date, ledger — all log-only) can never be satisfied
	// here: it would compile a log iterator and silently return unrelated/empty
	// results. Reject such conditions up front, deriving the verdict from the
	// same commonpb per-target validity table the compile layer uses, so the two
	// layers cannot drift.
	if err := rejectConditionsInvalidForAllTargets(filter, restPreparedQueryTargets); err != nil {
		return nil, err
	}

	return filter, nil
}

// restPreparedQueryTargets is the set of QueryTargets a REST prepared query can
// execute against, derived from preparedQueryTargets so the early-rejection walk
// and the target parser stay in lock-step. When a new target becomes REST-
// eligible (e.g. LOGS via EN-1503), adding it to preparedQueryTargets
// automatically relaxes the early rejection to match.
var restPreparedQueryTargets = func() []commonpb.QueryTarget {
	targets := make([]commonpb.QueryTarget, 0, len(preparedQueryTargets))
	for _, t := range preparedQueryTargets {
		targets = append(targets, t)
	}

	return targets
}()

// rejectConditionsInvalidForAllTargets walks the filter tree and rejects any
// leaf condition that is invalid on EVERY target in the given set, according to
// the shared commonpb validity table. A condition invalid on all reachable
// targets can never be satisfied, so admitting it would silently widen (or
// empty) results — invariant #7. Combinators are structural and always valid;
// the walk recurses into their children.
func rejectConditionsInvalidForAllTargets(f *commonpb.QueryFilter, targets []commonpb.QueryTarget) error {
	kind := commonpb.ConditionKindOf(f)

	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, sub := range v.And.GetFilters() {
			if err := rejectConditionsInvalidForAllTargets(sub, targets); err != nil {
				return err
			}
		}

		return nil
	case *commonpb.QueryFilter_Or:
		for _, sub := range v.Or.GetFilters() {
			if err := rejectConditionsInvalidForAllTargets(sub, targets); err != nil {
				return err
			}
		}

		return nil
	case *commonpb.QueryFilter_Not:
		return rejectConditionsInvalidForAllTargets(v.Not.GetFilter(), targets)
	}

	for _, t := range targets {
		if commonpb.ConditionValidForTarget(t, kind) {
			// Valid on at least one reachable target — a per-target check at
			// compile time will accept or reject it precisely. Not our concern.
			return nil
		}
	}

	return fmt.Errorf(
		"condition %q is only valid on log queries, which prepared queries cannot target over REST",
		kind.String())
}
