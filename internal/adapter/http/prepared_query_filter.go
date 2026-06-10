package http

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// decodePreparedQueryFilter decodes the `filter` JSON value of a prepared-query
// create/update request. The default `encoding/json` decoder cannot dispatch
// protobuf `oneof` variants, so we route the field through `protojson` to keep
// REST behaviour aligned with the gRPC path. An empty or absent filter is
// rejected — otherwise the prepared query would store nil and fail later at
// execute time with "unknown filter type: <nil>".
func decodePreparedQueryFilter(raw json.RawMessage) (*commonpb.QueryFilter, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, errors.New("filter is required")
	}

	filter := &commonpb.QueryFilter{}
	if err := protojson.Unmarshal(raw, filter); err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	if filter.GetFilter() == nil {
		return nil, errors.New("filter must contain at least one condition")
	}

	return filter, nil
}
