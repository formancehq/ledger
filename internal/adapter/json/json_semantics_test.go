package json

import (
	"bytes"
	standardjson "encoding/json"
	"reflect"
	"testing"
)

type parityFixture struct {
	Name     string         `json:"name"`
	Count    uint64         `json:"count"`
	Metadata map[string]any `json:"metadata"`
	Raw      RawValue       `json:"raw"`
}

func TestBackendMatchesStandardJSONForPortableValueShapes(t *testing.T) {
	want := parityFixture{
		Name:  "users:main",
		Count: 42,
		Metadata: map[string]any{
			"enabled": true,
			"labels":  []any{"one", "two"},
		},
		Raw: RawValue(`{"nested":"value"}`),
	}

	encoded, err := Marshal(want)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var gotValue, standardValue any
	if err := standardjson.Unmarshal(encoded, &gotValue); err != nil {
		t.Fatalf("standard json could not decode Marshal() output: %v", err)
	}
	standardEncoded, err := standardjson.Marshal(want)
	if err != nil {
		t.Fatalf("standard Marshal() error = %v", err)
	}
	if err := standardjson.Unmarshal(standardEncoded, &standardValue); err != nil {
		t.Fatalf("standard Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(gotValue, standardValue) {
		t.Fatalf("semantic JSON mismatch: got %#v want %#v", gotValue, standardValue)
	}

	var decoded parityFixture
	if err := Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("round trip mismatch: got %#v want %#v", decoded, want)
	}

	var stream bytes.Buffer
	if err := MarshalWrite(&stream, want); err != nil {
		t.Fatalf("MarshalWrite() error = %v", err)
	}
	var streamed parityFixture
	if err := UnmarshalRead(&stream, &streamed); err != nil {
		t.Fatalf("UnmarshalRead() error = %v", err)
	}
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("stream round trip mismatch: got %#v want %#v", streamed, want)
	}
}
