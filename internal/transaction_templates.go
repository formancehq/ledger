package ledger

import (
	"fmt"
	"slices"
)

type RuntimeType string

const (
	RuntimeMachine                 RuntimeType = "machine"
	RuntimeExperimentalInterpreter RuntimeType = "experimental-interpreter"
)

type TransactionTemplate struct {
	Description string      `json:"description"`
	Script      string      `json:"script"`
	Runtime     RuntimeType `json:"runtime,omitempty"`
}

type TransactionTemplates map[string]TransactionTemplate

func (t TransactionTemplates) Validate() error {
	for _, t := range t {
		if !slices.Contains([]RuntimeType{"", RuntimeMachine, RuntimeExperimentalInterpreter}, t.Runtime) {
			return fmt.Errorf("unexpected runtime `%s`: should be `%s` or `%s`", t.Runtime, RuntimeMachine, RuntimeExperimentalInterpreter)
		}
	}
	return nil
}
