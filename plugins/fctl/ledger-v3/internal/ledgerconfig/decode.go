package ledgerconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// DecodeFilter accepts the structured JSON filter DSL, a JSON-quoted textual
// filter, or the raw textual filter syntax used by command-line flags. Target
// validation remains authoritative on the Ledger server.
func DecodeFilter(raw []byte, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}

	switch trimmed[0] {
	case '{':
		filter := &commonpb.QueryFilter{}
		if err := json.Unmarshal(trimmed, filter); err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}
		if filter.GetFilter() == nil {
			return nil, errors.New("filter must contain at least one condition")
		}
		return filter, nil
	case '"':
		var expression string
		if err := json.Unmarshal(trimmed, &expression); err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}
		return parseFilterText(expression, target)
	default:
		return parseFilterText(string(trimmed), target)
	}
}

func parseFilterText(expression string, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	if expression == "" {
		return nil, nil
	}
	filter, err := Parse(expression, target)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	return filter, nil
}
