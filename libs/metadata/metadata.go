package metadata

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"

	"github.com/imdario/mergo"
)

const (
	namespace = "com.numary.spec/"
)

func SpecMetadata(name string) string {
	return namespace + name
}

type Metadata map[string]any

// IsEquivalentTo allow to compare to metadata object.
func (m1 Metadata) IsEquivalentTo(m2 Metadata) bool {
	return reflect.DeepEqual(m1, m2)
}

func (m1 Metadata) Merge(m2 Metadata) Metadata {
	ret := Metadata{}
	if err := mergo.Merge(&ret, m1, mergo.WithOverride); err != nil {
		panic(err)
	}
	if err := mergo.Merge(&ret, m2, mergo.WithOverride); err != nil {
		panic(err)
	}
	return ret
}

// Scan - Implement the database/sql scanner interface
func (m1 *Metadata) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	*m1 = Metadata{}
	switch vv := v.(type) {
	case []uint8:
		return json.Unmarshal(vv, m1)
	case string:
		return json.Unmarshal([]byte(vv), m1)
	default:
		panic("not handled type")
	}
}

func (m1 Metadata) ConvertValue(v interface{}) (driver.Value, error) {
	return json.Marshal(v)
}

func ComputeMetadata(key string, value interface{}) Metadata {
	return Metadata{
		key: value,
	}
}
