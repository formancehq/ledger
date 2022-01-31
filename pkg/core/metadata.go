package core

import (
	"encoding/json"
	"fmt"
)

type Metadata map[string]json.RawMessage

func (m Metadata) MarkRevertedBy(txID string) {
	m["state"] = []byte("\"reverted\"")
	m["state/reverted-by"] = []byte(fmt.Sprintf("\"%s\"", txID))
}

func (m Metadata) IsReverted() bool {
	return string(m["state"]) == "\"reverted\""
}
