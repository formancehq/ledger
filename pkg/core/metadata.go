package core

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

const (
	numaryNamespace           = "com.numary"
	revertKey                 = numaryNamespace + ".spec/state/reverts"
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

type Metadata map[string]json.RawMessage

func (m Metadata) MarkReverts(txID uint64) {
	m[revertKey] = json.RawMessage(fmt.Sprintf(`"%d"`, txID))
}

func (m Metadata) IsReverted() bool {
	return string(m["state"]) == "\"reverted\""
}

// Scan - Implement the database/sql scanner interface
func (m *Metadata) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	*m = Metadata{}
	switch vv := v.(type) {
	case []uint8:
		return json.Unmarshal(vv, m)
	case string:
		return json.Unmarshal([]byte(vv), m)
	default:
		panic("not handled type")
	}
}

func (m Metadata) ConvertValue(v interface{}) (driver.Value, error) {
	return json.Marshal(v)
}
