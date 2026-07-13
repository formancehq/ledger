package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// preparedQueryTargets maps the public REST target enum to the proto
// QueryTarget. ACCOUNTS, TRANSACTIONS and LOGS are all usable prepared-query
// targets over REST: query.Execute hydrates the matching cursor field
// (account_data / transaction_data / log_data) for each. See openapi.yml.
var preparedQueryTargets = map[string]commonpb.QueryTarget{
	"ACCOUNTS":     commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	"TRANSACTIONS": commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	"LOGS":         commonpb.QueryTarget_QUERY_TARGET_LOGS,
}

// parsePreparedQueryTarget resolves the `target` field of a prepared-query
// request. An unknown or unsupported value is rejected loudly rather than
// silently coerced to a default (which would run a different query than the
// caller asked for).
func parsePreparedQueryTarget(target string) (commonpb.QueryTarget, error) {
	if target == "" {
		return 0, errors.New("target is required (ACCOUNTS, TRANSACTIONS or LOGS)")
	}

	t, ok := preparedQueryTargets[target]
	if !ok {
		return 0, fmt.Errorf("unknown or unsupported target %q (use ACCOUNTS, TRANSACTIONS or LOGS)", target)
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

	// Per-target validity (a condition must be valid on the query's specific
	// target — logId/date/ledger are valid on LOGS but not on
	// ACCOUNTS/TRANSACTIONS) is enforced by domain.ValidateFilterForTarget at the
	// admission + FSM layers, which know the concrete target for both create
	// (from the request) and update (from the stored query). The create handler
	// also runs it here for an early, precise 400; this decoder stays purely
	// structural.
	return filter, nil
}
