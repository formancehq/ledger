package common

import "encoding/json"

type RunQuery struct {
	Params json.RawMessage   `json:"params,omitempty"`
	Vars   map[string]string `json:"vars,omitempty"`
	Cursor *string           `json:"cursor,omitempty"`
}
