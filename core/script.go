package core

import "encoding/json"

type Script struct {
	Reference string                     `json:"reference"`
	Plain     string                     `json:"plain"`
	Vars      map[string]json.RawMessage `json:"vars"`
}
