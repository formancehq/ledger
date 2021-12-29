package core

import (
	"encoding/json"
	"fmt"
)

type Metadata map[string]json.RawMessage

func (m Metadata) MarkRevertedBy(txID string) {
	m["scheme/state"] = []byte("\"reverted\"")
	m["scheme/state/reverted-by"] = []byte(fmt.Sprintf("\"%s\"", txID))
}

func (m Metadata) IsReverted() bool {
	return string(m["scheme/state"]) == "\"reverted\""
}
