package ledger

import (
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type RunScript struct {
	Script
	Timestamp Time              `json:"timestamp"`
	Metadata  metadata.Metadata `json:"metadata"`
	Reference string            `json:"reference"`
}

type Script struct {
	Plain string            `json:"plain"`
	Vars  map[string]string `json:"vars" swaggertype:"object"`
}
