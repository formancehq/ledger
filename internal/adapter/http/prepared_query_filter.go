package http

import (
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
