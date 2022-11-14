package core

import "encoding/json"

type Script struct {
	ScriptCore
	Reference string   `json:"reference"`
	Metadata  Metadata `json:"metadata"`
}

type ScriptCore struct {
	Plain string                     `json:"plain"`
	Vars  map[string]json.RawMessage `json:"vars" swaggertype:"object"`
}
