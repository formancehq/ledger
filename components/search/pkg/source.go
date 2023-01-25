package search

import (
	"encoding/json"
	"time"
)

type Source struct {
	ID     string          `json:"_id"`
	Kind   string          `json:"kind"`
	Ledger string          `json:"ledger"`
	When   time.Time       `json:"when"`
	Data   json.RawMessage `json:"data"`
}
