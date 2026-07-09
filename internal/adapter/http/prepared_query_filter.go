package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// preparedQueryTargets maps the public REST target enum
// (ACCOUNTS/TRANSACTIONS/LOGS, per openapi.yml) to the proto QueryTarget. All
// three targets are supported by the query executor and creatable via
// gRPC/CLI, so REST must accept them too.
var preparedQueryTargets = map[string]commonpb.QueryTarget{
	"ACCOUNTS":     commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	"TRANSACTIONS": commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	"LOGS":         commonpb.QueryTarget_QUERY_TARGET_LOGS,
}

// parsePreparedQueryTarget resolves the `target` field of a prepared-query
// request. An unknown value is rejected loudly rather than silently coerced to
// a default (which would run a different query than the caller asked for).
func parsePreparedQueryTarget(target string) (commonpb.QueryTarget, error) {
	if target == "" {
		return 0, errors.New("target is required (ACCOUNTS, TRANSACTIONS or LOGS)")
	}

	t, ok := preparedQueryTargets[target]
	if !ok {
		return 0, fmt.Errorf("unknown target %q (use ACCOUNTS, TRANSACTIONS or LOGS)", target)
	}

	return t, nil
}

// decodePreparedQueryFilter decodes the `filter` JSON value of a prepared-query
// create/update request. The value uses the canonical flat QueryFilter shape
// documented in openapi.yml (combinators `and`/`or`/`not` plus a tagged-union
// `match`); the codec lives on commonpb.QueryFilter (query_filter.go) and keeps
// the protobuf-internal oneof/wrapper names off the public surface. An empty or
// absent filter is rejected — otherwise the prepared query would store nil and
// fail later at execute time with "unknown filter type: <nil>".
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

	return filter, nil
}
