package auth

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

type Array[T any] []T

// Scan implements the sql.Scanner interface.
func (a *Array[T]) Scan(src interface{}) error {
	*a = make(Array[T], 0)
	var err error
	switch src := src.(type) {
	case []byte:
		err = json.Unmarshal(src, a)
	case string:
		err = json.Unmarshal([]byte(src), a)
	case nil:
	default:
		return fmt.Errorf("type '%T' not handled", src)
	}
	if err != nil {
		return err
	}
	return nil
}

func (a *Array[T]) Contains(t T) bool {
	for _, v := range *a {
		if reflect.DeepEqual(v, t) {
			return true
		}
	}
	return false
}

func (a *Array[T]) Append(t T) *Array[T] {
	*a = append(*a, t)
	return a
}

// Value implements the driver.Valuer interface.
func (a Array[T]) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	return json.Marshal(a)
}
