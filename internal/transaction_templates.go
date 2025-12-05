package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
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

// Marshal that transforms a list of transactions with ids into a map
func (t *TransactionTemplates) UnmarshalJSON(data []byte) error {
	var rawList []struct {
		ID          string
		Description string
		Script      string
	}
	if err := json.Unmarshal(data, &rawList); err != nil {
		return err
	}
	out := make(map[string]TransactionTemplate)
	for _, item := range rawList {
		if item.ID == "" {
			return errors.New("transaction template id cannot be empty")
		}
		if _, exists := out[item.ID]; exists {
			return fmt.Errorf("duplicate transaction template id: %v", item.ID)
		}
		out[item.ID] = TransactionTemplate{
			Description: item.Description,
			Script:      item.Script,
		}
	}
	*t = out
	return nil
}

func (t TransactionTemplates) MarshalJSON() ([]byte, error) {
	var rawList []struct {
		ID          string
		Description string
		Script      string
	}
	for id, item := range t {
		rawList = append(rawList, struct {
			ID          string
			Description string
			Script      string
		}{
			ID:          id,
			Description: item.Description,
			Script:      item.Script,
		})
	}
	return json.Marshal(rawList)
}
