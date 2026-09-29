package audit

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Canonicalize returns the canonical serialisation of a JSON document:
// two-space indentation, object keys in lexicographic order, HTML escaping
// off, and exactly one trailing newline.
//
// Decoding into any + re-encoding is what sorts the keys: encoding/json emits
// map keys in sorted order. That makes the form independent of how the
// document was produced, which is the property the determinism gate needs — a
// regenerated document and a hand-edited one converge on the same bytes or
// the audit fails.
func Canonicalize(raw []byte) ([]byte, error) {
	var doc any

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // preserve numeric literals instead of round-tripping through float64

	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	if err := enc.Encode(doc); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	return buf.Bytes(), nil
}

// IsCanonical reports whether raw already equals its canonical form.
func IsCanonical(raw []byte) (bool, []byte, error) {
	want, err := Canonicalize(raw)
	if err != nil {
		return false, nil, err
	}

	return bytes.Equal(raw, want), want, nil
}
