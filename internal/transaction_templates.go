package ledger

import (
	"encoding/json"
)

type RuntimeType string

const (
	RuntimeExperimentalInterpreter RuntimeType = "experimental-interpreter"
	RuntimeMachine                 RuntimeType = "machine"
)

type TransactionTemplate struct {
	Description string
	Script      string
	Runtime     RuntimeType
}

type TransactionTemplates map[string]TransactionTemplate

func (t *TransactionTemplates) UnmarshalJSON(data []byte) error {
	type Templates TransactionTemplates
	var templates Templates
	if err := json.Unmarshal(data, &templates); err != nil {
		return err
	}
	for id, tmpl := range templates {
		if tmpl.Runtime == "" {
			tmpl.Runtime = RuntimeMachine
			templates[id] = tmpl
		}
	}
	*t = TransactionTemplates(templates)
	return nil
}
