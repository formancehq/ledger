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

func (s *RunScript) WithDefaultValues() {
	if s.Timestamp.IsZero() {
		s.Timestamp = Now()
	} else {
		s.Timestamp = s.Timestamp.UTC().Round(DatePrecision)
	}
}

type Script struct {
	Plain string                     `json:"plain"`
	Vars  map[string]json.RawMessage `json:"vars" swaggertype:"object"`
}
