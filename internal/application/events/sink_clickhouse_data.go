//go:build clickhouse

package events

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// eventToClickHouseJSON quotes only posting amounts in the analytical projection.
// The native driver sends this document as a JSON string: ClickHouse 24.x can
// reject integer tokens above UInt64 before applying the UInt256 column hint.
// Quoted decimals bypass that parser limit and retain the numeric column type.
// RawMessages preserve every other numeric token without a float64 round trip.
func eventToClickHouseJSON(event *eventspb.Event) ([]byte, error) {
	data, err := eventToSinkJSON(event)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for _, key := range []string{"transaction", "revertTransaction"} {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		converted, err := clickHouseTransactionJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		fields[key] = converted
	}

	return json.Marshal(fields)
}

func clickHouseTransactionJSON(raw json.RawMessage) (json.RawMessage, error) {
	var transaction map[string]json.RawMessage
	if err := json.Unmarshal(raw, &transaction); err != nil {
		return nil, err
	}
	var postings []map[string]json.RawMessage
	if err := json.Unmarshal(transaction["postings"], &postings); err != nil {
		return nil, err
	}
	for i, posting := range postings {
		amount := posting["amount"]
		if len(amount) == 0 {
			return nil, fmt.Errorf("posting %d: missing amount", i)
		}
		for _, digit := range amount {
			if digit < '0' || digit > '9' {
				return nil, fmt.Errorf("posting %d: expected unsigned decimal amount", i)
			}
		}
		quoted, err := json.Marshal(string(amount))
		if err != nil {
			return nil, err
		}
		posting["amount"] = quoted
	}
	converted, err := json.Marshal(postings)
	if err != nil {
		return nil, err
	}
	transaction["postings"] = converted

	return json.Marshal(transaction)
}
