package processor

import (
	"github.com/formancehq/orchestration/internal/spec"
)

type Input struct {
	Specification spec.Specification `json:"spec"`
	Parameters    any                `json:"parameters"`
	Variables     map[string]string  `json:"variables"`
}
