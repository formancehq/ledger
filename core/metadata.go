package core

import "encoding/json"

type MetaEntry struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type Metadata map[string]MetaEntry
