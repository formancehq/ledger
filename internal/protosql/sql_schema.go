package protosql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/invopop/jsonschema"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// VolumesAdapter keeps database encoding on the server side of the public
// Protobuf contract.
type VolumesAdapter struct {
	Volumes *commonpb.Volumes
}

var _ driver.Valuer = VolumesAdapter{}
var _ sql.Scanner = VolumesAdapter{}

func (a VolumesAdapter) Value() (driver.Value, error) {
	if a.Volumes == nil {
		return nil, nil
	}

	return fmt.Sprintf("(%s, %s)", a.Volumes.GetInput(), a.Volumes.GetOutput()), nil
}

func (a VolumesAdapter) Scan(src any) error {
	if src == nil {
		return nil
	}
	if a.Volumes == nil {
		return errors.New("Volumes.Scan: nil destination")
	}
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("Volumes.Scan: expected string, got %T", src)
	}
	if len(s) < 3 || s[0] != '(' || s[len(s)-1] != ')' {
		return fmt.Errorf("Volumes.Scan: invalid volume pair %q", s)
	}
	parts := strings.Split(s[1:len(s)-1], ",")
	if len(parts) != 2 {
		return fmt.Errorf("Volumes.Scan: invalid volume pair %q", s)
	}

	a.Volumes.Input = strings.TrimSpace(parts[0])
	a.Volumes.Output = strings.TrimSpace(parts[1])

	return nil
}

// ExtendVolumesJSONSchema adds the derived balance property to a server schema.
func ExtendVolumesJSONSchema(schema *jsonschema.Schema) {
	inputProperty, _ := schema.Properties.Get("input")
	schema.Properties.Set("balance", inputProperty)
}

// LogTypeAdapter keeps SQL conversion separate from the public JSON log type.
type LogTypeAdapter struct {
	LogType *commonpb.LogType
}

var _ driver.Valuer = LogTypeAdapter{}
var _ sql.Scanner = LogTypeAdapter{}

func (a LogTypeAdapter) Value() (driver.Value, error) {
	if a.LogType == nil {
		return nil, errors.New("LogType.Value: nil value")
	}

	return a.LogType.String(), nil
}

func (a LogTypeAdapter) Scan(src any) error {
	if a.LogType == nil {
		return errors.New("LogType.Scan: nil destination")
	}
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("LogType.Scan: expected string, got %T", src)
	}
	v, err := commonpb.LogTypeFromString(s)
	if err != nil {
		return err
	}
	*a.LogType = v

	return nil
}
