package core

import (
	"encoding/json"
	"time"
)

type ScriptData struct {
	Script
	Timestamp time.Time `json:"timestamp"`
	Reference string    `json:"reference"`
	Metadata  Metadata  `json:"metadata"`
}

type Script struct {
	Plain string                     `json:"plain"`
	Vars  map[string]json.RawMessage `json:"vars" swaggertype:"object"`
}
