package auth

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type Metadata map[string]string

// Scan implements the sql.Scanner interface.
func (a *Metadata) Scan(src interface{}) error {
	*a = Metadata{}
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

// Value implements the driver.Valuer interface.
func (a Metadata) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	return json.Marshal(a)
}
