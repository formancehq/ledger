package core

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

const (
	numaryNamespace           = "com.numary.spec/"
	revertKey                 = "state/reverts"
	revertedKey               = "state/reverted"
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

func SpecMetadata(name string) string {
	return numaryNamespace + name
}

type Metadata map[string]any

// IsEquivalentTo allow to compare to metadata object.
func (m1 Metadata) IsEquivalentTo(m2 Metadata) bool {
	return reflect.DeepEqual(m1, m2)
}

func (m1 Metadata) Merge(m2 Metadata) Metadata {
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
}

func (m Metadata) MarkReverts(txID uint64) {
	m.Merge(RevertMetadata(txID))
}

func (m Metadata) IsReverted() bool {
	return m[SpecMetadata(revertedKey)].(string) == "\"reverted\""
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

type RevertedMetadataSpecValue struct {
	By string `json:"by"`
}

func RevertedMetadataSpecKey() string {
	return SpecMetadata(revertedKey)
}

func RevertMetadataSpecKey() string {
	return SpecMetadata(revertKey)
}

func ComputeMetadata(key string, value interface{}) Metadata {
	return Metadata{
		key: value,
	}
}

func RevertedMetadata(by uint64) Metadata {
	return ComputeMetadata(RevertedMetadataSpecKey(), RevertedMetadataSpecValue{
		By: fmt.Sprint(by),
	})
}

func RevertMetadata(tx uint64) Metadata {
	return ComputeMetadata(RevertMetadataSpecKey(), fmt.Sprint(tx))
}
