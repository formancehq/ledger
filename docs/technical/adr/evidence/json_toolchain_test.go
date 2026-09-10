//go:build json_toolchain_evidence

// Run explicitly by filename from the repository root; this opt-in ADR fixture
// is not part of the production package or the default unit-test inventory.
package evidence

import (
	"bytes"
	jsonv1 "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type framing struct {
	Text  string         `json:"text"`
	Keys  map[string]int `json:"keys"`
	Slice []string       `json:"slice"`
	Map   map[string]int `json:"map"`
	Zero  int            `json:"zero,omitempty"`
	False bool           `json:"false,omitempty"`
}

func transactions() []*commonpb.Transaction {
	result := make([]*commonpb.Transaction, 100)
	for i := range result {
		result[i] = &commonpb.Transaction{
			Id:        uint64(i + 1),
			Reference: fmt.Sprintf("invoice-%d", i),
			Timestamp: &commonpb.Timestamp{Data: 1788825600000000},
			Postings: []*commonpb.Posting{{
				Source: "world", Destination: "users:alice", Asset: "USD/2", Color: "",
				Amount: commonpb.NewUint256FromUint64(9007199254740993),
			}},
			Metadata: map[string]*commonpb.MetadataValue{
				"reference": commonpb.NewStringValue("invoice"),
				"customer":  commonpb.NewStringValue("alice"),
			},
		}
	}
	return result
}

func TestCompatibility(t *testing.T) {
	t.Parallel()
	t.Logf("toolchain=%s platform=%s/%s sonic.APIKind=%d (1=native)", runtime.Version(), runtime.GOOS, runtime.GOARCH, sonic.APIKind)
	if sonic.APIKind != sonic.UseSonicJSON {
		t.Fatal("expected native Sonic baseline")
	}
	value := framing{Text: "<>&\u2028\u2029", Keys: map[string]int{"z": 1, "a": 2, "m": 3}}
	defaultShapes := map[string]bool{}
	for i := 0; i < 64; i++ {
		data, err := sonic.ConfigDefault.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		defaultShapes[string(data)] = true
	}
	for data := range defaultShapes {
		t.Logf("Sonic Default Marshal=%s", data)
	}
	var stream bytes.Buffer
	if err := sonic.ConfigStd.NewEncoder(&stream).Encode(value); err != nil {
		t.Fatal(err)
	}
	t.Logf("Sonic Std Encode=%q", stream.String())
	v2, err := jsonv2.Marshal(value, jsonv1.DefaultOptionsV1())
	if err != nil {
		t.Fatal(err)
	}
	if stream.String() != string(v2)+"\n" {
		t.Fatalf("plain Std mismatch: %q vs %q", stream.String(), v2)
	}
	t.Logf("v2 DefaultOptionsV1 + newline equals plain Sonic Std")
	opts := []jsonv2.Options{jsonv1.DefaultOptionsV1(), jsonv2.Deterministic(false), jsontext.EscapeForHTML(false), jsontext.EscapeForJS(false)}
	configured, err := jsonv2.Marshal(value, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("v2 native-Default-shaped options=%s", configured)
	for _, required := range []string{"<>&\u2028\u2029", `"slice":null`, `"map":null`} {
		if !strings.Contains(string(configured), required) {
			t.Fatalf("missing %q: %s", required, configured)
		}
	}
	for _, omitted := range []string{`"zero":`, `"false":`} {
		if strings.Contains(string(configured), omitted) {
			t.Fatalf("unexpected %q: %s", omitted, configured)
		}
	}
	var configuredValue any
	if err := jsonv1.Unmarshal(configured, &configuredValue); err != nil {
		t.Fatal(err)
	}
	for encoded := range defaultShapes {
		if !strings.Contains(encoded, "<>&\u2028\u2029") {
			t.Fatalf("native escape mismatch: %s", encoded)
		}
		var sonicValue any
		if err := jsonv1.Unmarshal([]byte(encoded), &sonicValue); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(configuredValue, sonicValue) {
			t.Fatalf("native semantics mismatch: %s", encoded)
		}
	}
	bare, err := jsonv2.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("v2 bare=%s", bare)
	tx := transactions()[0]
	tx.Reference = "<>&\u2028\u2029"
	tx.Metadata = map[string]*commonpb.MetadataValue{"z": commonpb.NewStringValue("last"), "a": commonpb.NewStringValue("first"), "m": commonpb.NewStringValue("middle")}
	nestedShapes := map[string]bool{}
	for i := 0; i < 64; i++ {
		stream.Reset()
		if err := sonic.ConfigStd.NewEncoder(&stream).Encode(tx); err != nil {
			t.Fatal(err)
		}
		nestedShapes[stream.String()] = true
	}
	for data := range nestedShapes {
		t.Logf("nested Sonic Std=%q", data)
	}
	var v2stream bytes.Buffer
	if err := jsonv2.MarshalWrite(&v2stream, tx, jsonv1.DefaultOptionsV1()); err != nil {
		t.Fatal(err)
	}
	if err := v2stream.WriteByte('\n'); err != nil {
		t.Fatal(err)
	}
	t.Logf("nested v2 V1-options stream=%q", v2stream.String())
	var v2nested any
	if err := jsonv1.Unmarshal(v2stream.Bytes(), &v2nested); err != nil {
		t.Fatal(err)
	}
	for encoded := range nestedShapes {
		var sonicNested any
		if err := jsonv1.Unmarshal([]byte(encoded), &sonicNested); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(v2nested, sonicNested) {
			t.Fatalf("nested semantics mismatch: %s", encoded)
		}
	}
	if !strings.Contains(v2stream.String(), `\u003c\u003e\u0026\u2028\u2029`) {
		t.Fatalf("nested escape mismatch: %s", v2stream.String())
	}
	t.Logf("distinct Default shapes=%d nested Std shapes=%d (sampled, not a portable ordering guarantee)", len(defaultShapes), len(nestedShapes))
	calls := 0
	override := jsonv2.WithMarshalers(jsonv2.MarshalFunc(func(v *commonpb.Uint256) ([]byte, error) { calls++; return []byte(`"` + v.Dec() + `"`), nil }))
	direct, err := jsonv2.Marshal(tx.Postings[0].Amount, override)
	if err != nil {
		t.Fatal(err)
	}
	if string(direct) != `"9007199254740993"` || calls != 1 {
		t.Fatal("direct override not exercised")
	}
	calls = 0
	nested, err := jsonv2.Marshal(tx, override, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || !strings.Contains(string(nested), `"amount":9007199254740993`) {
		t.Fatalf("unexpected nested override behavior calls=%d data=%s", calls, nested)
	}
	t.Logf("Uint256 override direct=%s; nested calls=%d output=%s", direct, calls, nested)
}

// Transaction is a response projection with flattened MetadataValue oneofs;
// decoding that wire representation directly into protobuf fields is invalid.
// Use the same typed wire DTO for all decoders, without custom unmarshal hooks.
type transactionWire struct {
	Postings []struct {
		Source      string        `json:"source"`
		Destination string        `json:"destination"`
		Amount      jsonv1.Number `json:"amount"`
		Asset       string        `json:"asset"`
		Color       string        `json:"color"`
	} `json:"postings"`
	Metadata  map[string]string `json:"metadata"`
	Timestamp string            `json:"timestamp"`
	Reference string            `json:"reference"`
	ID        uint64            `json:"id"`
	Reverted  bool              `json:"reverted"`
}

func BenchmarkTransactionList(b *testing.B) {
	value := transactions()
	input, err := sonic.ConfigDefault.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("100 real commonpb transactions; Sonic Default input bytes=%d; existing nested MarshalJSON retained; decode uses typed wire DTO", len(input))
	codecs := []struct {
		name      string
		marshal   func(any) ([]byte, error)
		write     func(io.Writer, any) error
		unmarshal func([]byte, any) error
	}{
		{"stdlib-v1", jsonv1.Marshal, func(w io.Writer, v any) error { return jsonv1.NewEncoder(w).Encode(v) }, jsonv1.Unmarshal},
		{"sonic-native", sonic.ConfigDefault.Marshal, func(w io.Writer, v any) error { return sonic.ConfigStd.NewEncoder(w).Encode(v) }, sonic.ConfigDefault.Unmarshal},
		{"stdlib-v2-v1-options", func(v any) ([]byte, error) { return jsonv2.Marshal(v, jsonv1.DefaultOptionsV1()) }, func(w io.Writer, v any) error {
			if err := jsonv2.MarshalWrite(w, v, jsonv1.DefaultOptionsV1()); err != nil {
				return err
			}
			_, err := io.WriteString(w, "\n")
			return err
		}, func(data []byte, v any) error { return jsonv2.Unmarshal(data, v, jsonv1.DefaultOptionsV1()) }},
		{"stdlib-v2-native-default-options", func(v any) ([]byte, error) {
			return jsonv2.Marshal(v, jsonv1.DefaultOptionsV1(), jsonv2.Deterministic(false), jsontext.EscapeForHTML(false), jsontext.EscapeForJS(false))
		}, nil, nil},
	}
	for _, codec := range codecs {
		b.Run(codec.name+"/Marshal", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := codec.marshal(value); err != nil {
					b.Fatal(err)
				}
			}
		})
		if codec.write != nil {
			b.Run(codec.name+"/Write", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := codec.write(io.Discard, value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		if codec.unmarshal != nil {
			b.Run(codec.name+"/Unmarshal", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var decoded []transactionWire
					if err := codec.unmarshal(input, &decoded); err != nil {
						b.Fatal(err)
					}
					if len(decoded) != 100 || decoded[0].Postings[0].Amount.String() != "9007199254740993" {
						b.Fatal("decode fixture mismatch")
					}
				}
			})
		}
	}
}
