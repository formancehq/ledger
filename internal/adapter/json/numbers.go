package json

import (
	"io"

	"github.com/bytedance/sonic"
)

var numberDecoder = sonic.Config{UseNumber: true}.Froze()

// UnmarshalUseNumber preserves numeric tokens as encoding/json.Number in
// interface values. Use it only where the consumer explicitly handles Number;
// the shared Unmarshal default remains unchanged.
func UnmarshalUseNumber(data []byte, v any) error {
	return numberDecoder.Unmarshal(data, v)
}

// UnmarshalReadUseNumber is UnmarshalRead with exact numeric tokens in interface
// values. Typed fields and custom UnmarshalJSON methods retain their own rules.
func UnmarshalReadUseNumber(r io.Reader, v any) error {
	decoder := sonic.ConfigStd.NewDecoder(r)
	decoder.UseNumber()

	return decoder.Decode(v)
}
