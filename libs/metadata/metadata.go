package metadata

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"

	"github.com/imdario/mergo"
)

type Metadata map[string]string

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

func (m1 Metadata) Copy() Metadata {
	ret := Metadata{}
	for k, v := range m1 {
		ret[k] = v
	}
	return ret
}

func ComputeMetadata(key, value string) Metadata {
	return Metadata{
		key: value,
	}
}

func MarshalValue(v any) string {
	vv, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return string(vv)
}

func UnmarshalValue[TO any](value string) TO {
	var ret TO
	if err := json.Unmarshal([]byte(value), &ret); err != nil {
		panic(err)
	}
	return ret
}
