package core

type MetaEntry struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type Metadata map[string]MetaEntry
