package core

import (
	"encoding/json"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type RunScript struct {
	Script
	Timestamp Time              `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata"`
}

type Script struct {
	Plain string                     `json:"plain"`
	Vars  map[string]json.RawMessage `json:"vars" swaggertype:"object"`
}
