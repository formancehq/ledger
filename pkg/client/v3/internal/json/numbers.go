package json

import (
	"bytes"
	stdjson "encoding/json"
	"io"
)

// UnmarshalUseNumber preserves numeric tokens as encoding/json.Number in
// interface values. Use it only where the consumer explicitly handles Number;
// the shared Unmarshal default remains unchanged.
func UnmarshalUseNumber(data []byte, v any) error {
	decoder := stdjson.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	return decoder.Decode(v)
}

// UnmarshalReadUseNumber is UnmarshalRead with exact numeric tokens in interface
// values. Typed fields and custom UnmarshalJSON methods retain their own rules.
func UnmarshalReadUseNumber(r io.Reader, v any) error {
	decoder := stdjson.NewDecoder(r)
	decoder.UseNumber()

	return decoder.Decode(v)
}
