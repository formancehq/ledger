package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

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
