//go:build fctl_component_guest

// Package json provides the Ledger JSON compatibility surface for portable
// component guests without embedding Sonic's native JIT runtime.
package json

import (
	standardjson "encoding/json"
	"io"
)

const backendName = "stdlib"

func Marshal(v any) ([]byte, error) {
	return standardjson.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	return standardjson.Unmarshal(data, v)
}

func UnmarshalRead(r io.Reader, v any) error {
	return standardjson.NewDecoder(r).Decode(v)
}

func MarshalWrite(w io.Writer, v any) error {
	return standardjson.NewEncoder(w).Encode(v)
}

type RawValue []byte

func (r RawValue) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return r, nil
}

func (r *RawValue) UnmarshalJSON(data []byte) error {
	if r == nil {
		return nil
	}
	*r = append((*r)[:0], data...)
	return nil
}

func (r RawValue) String() string {
	return string(r)
}
