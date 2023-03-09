package core

import (
	"encoding/json"
)

type RunScript struct {
	Script
	Timestamp Time     `json:"timestamp"`
	Reference string   `json:"reference"`
	Metadata  Metadata `json:"metadata"`
}

type Script struct {
	Plain string                     `json:"plain"`
	Vars  map[string]json.RawMessage `json:"vars" swaggertype:"object"`
}
