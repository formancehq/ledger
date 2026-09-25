package common

import (
	"bytes"
	"encoding/json"
)

type RunQuery struct {
	Params json.RawMessage `json:"params,omitempty"`
	Vars   map[string]any  `json:"vars,omitempty"`
	Cursor *string         `json:"cursor,omitempty"`
}

// UnmarshalJSON decodes numeric vars as json.Number, like template defaults,
// so integers above 2^53 are not rounded to float64.
func (q *RunQuery) UnmarshalJSON(data []byte) error {
	type runQuery RunQuery
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode((*runQuery)(q))
}
