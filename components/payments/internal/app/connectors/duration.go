package connectors

import (
	"encoding/json"
	"fmt"
	"time"
)

type Duration struct {
	time.Duration `json:"duration"`
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var rawValue any

	if err := json.Unmarshal(b, &rawValue); err != nil {
		return fmt.Errorf("custom Duration UnmarshalJSON: %w", err)
	}

	switch value := rawValue.(type) {
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("custom Duration UnmarshalJSON: time.ParseDuration: %w", err)
		}

		return nil
	case map[string]interface{}:
		switch val := value["duration"].(type) {
		case float64:
			d.Duration = time.Duration(int64(val))

			return nil
		default:
			return fmt.Errorf("custom Duration UnmarshalJSON from map: invalid type: value:%v, type:%T", val, val)
		}
	default:
		return fmt.Errorf("custom Duration UnmarshalJSON: invalid type: value:%v, type:%T", value, value)
	}
}
