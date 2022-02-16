package core

import (
	"encoding/json"
)

const numaryNamespace = "com.numary"

const revertKey = numaryNamespace + ".spec/state/reverts"

type Metadata map[string]json.RawMessage

func (m Metadata) MarkReverts(txID string) {
	m[revertKey] = []byte(txID)
}

func (m Metadata) IsReverted() bool {
	return string(m["state"]) == "\"reverted\""
}
