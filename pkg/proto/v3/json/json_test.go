package json

import (
	"bytes"
	stdjson "encoding/json"
	"strings"
	"testing"
)

func TestRawValueZeroValueMarshalsAsNull(t *testing.T) {
	data, err := stdjson.Marshal(struct {
		Value RawValue `json:"value"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"value":null}`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestUnmarshalUseNumberRejectsTrailingData(t *testing.T) {
	var value map[string]any
	if err := UnmarshalUseNumber([]byte(`{"value":1}{"other":2}`), &value); err == nil {
		t.Fatal("expected trailing JSON value to be rejected")
	}
	if err := UnmarshalUseNumber([]byte(`{"value":1}junk`), &value); err == nil {
		t.Fatal("expected trailing non-whitespace data to be rejected")
	}
}

func TestDefaultCodecPreservesV1ReaderAndWriterBehavior(t *testing.T) {
	var value struct {
		Count int `json:"count"`
	}
	if err := UnmarshalRead(strings.NewReader(`{"COUNT":7}`), &value); err != nil {
		t.Fatal(err)
	}
	if value.Count != 7 {
		t.Fatalf("got count %d, want 7", value.Count)
	}

	var output bytes.Buffer
	if err := MarshalWrite(&output, []int(nil)); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), "null\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
