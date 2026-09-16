package ledger

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestChartUnknownPropertiesAreIgnored(t *testing.T) {
	t.Parallel()
	keys := []string{".patern", ".metadatas", ".rulez", ".forbiden", ".anything-at-all", "."}
	values := map[string]json.RawMessage{
		"object": json.RawMessage(`{}`),
		"string": json.RawMessage(`"^[A-Z]+$"`),
		"number": json.RawMessage(`42`),
		"bool":   json.RawMessage(`true`),
		"array":  json.RawMessage(`[1,"x"]`),
		"null":   json.RawMessage(`null`),
	}
	for _, key := range keys {
		for valueName, value := range values {
			for _, location := range []string{"root-child", "nested-fixed", "variable", "non-account", "self-account"} {
				t.Run(key+"/"+valueName+"/"+location, func(t *testing.T) {
					t.Parallel()
					segment := map[string]any{key: value}
					var input map[string]any
					var wantJSON string
					switch location {
					case "root-child":
						input = map[string]any{"bank": segment}
						wantJSON = `{"bank":{}}`
					case "nested-fixed":
						input = map[string]any{"bank": map[string]any{"deposit": segment}}
						wantJSON = `{"bank":{"deposit":{}}}`
					case "variable":
						segment[PATTERN_KEY] = "^[A-Z]+$"
						input = map[string]any{"bank": map[string]any{"$id": segment}}
						wantJSON = `{"bank":{"$id":{".pattern":"^[A-Z]+$"}}}`
					case "non-account":
						segment["child"] = map[string]any{}
						input = map[string]any{"bank": segment}
						wantJSON = `{"bank":{"child":{}}}`
					case "self-account":
						segment[SELF_KEY] = map[string]any{}
						segment["child"] = map[string]any{}
						input = map[string]any{"bank": segment}
						wantJSON = `{"bank":{".self":{},"child":{}}}`
					}
					payload, err := json.Marshal(input)
					if err != nil {
						t.Fatal(err)
					}
					var chart, want ChartOfAccounts
					if err := json.Unmarshal(payload, &chart); err != nil {
						t.Fatalf("previously accepted chart %s failed: %v", payload, err)
					}
					if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(want, chart) {
						t.Fatalf("unknown property changed chart meaning: got %#v, want %#v", chart, want)
					}
					encoded, err := json.Marshal(chart)
					if err != nil {
						t.Fatal(err)
					}
					if string(encoded) != wantJSON {
						t.Fatalf("unknown property changed serialization: got %s, want %s", encoded, wantJSON)
					}
				})
			}
		}
	}
}

func TestChartKnownPropertiesRetainMeaning(t *testing.T) {
	t.Parallel()
	input := `{"bank":{".patern":{},".self":{},".rules":{},".metadata":{".business-key":{"default":"x"}},"$id":{".rulez":true,".pattern":"^[A-Z]{4}$",".self":{},".metadata":{"region":{"default":"EU"}},"child":{}}}}`
	var chart ChartOfAccounts
	if err := json.Unmarshal([]byte(input), &chart); err != nil {
		t.Fatal(err)
	}
	if chart["bank"].Account == nil {
		t.Fatal(".self account was lost")
	}
	variable := chart["bank"].VariableSegment
	if variable == nil || variable.Pattern == nil || *variable.Pattern != "^[A-Z]{4}$" {
		t.Fatal("parent-owned .pattern was lost")
	}
	if variable.Account == nil {
		t.Fatal("variable segment's .self account was lost")
	}
	if got := variable.Account.Metadata["region"].Default; got == nil || *got != "EU" {
		t.Fatal("variable segment's default metadata was lost")
	}
	if got := chart["bank"].Account.Metadata[".business-key"].Default; got == nil || *got != "x" {
		t.Fatal("metadata keys must not be interpreted as chart directives")
	}
	payload, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip ChartOfAccounts
	if err := json.Unmarshal(payload, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(chart, roundtrip) {
		t.Fatal("known properties changed during round trip")
	}
}

func TestChartPatternValidationUnchanged(t *testing.T) {
	t.Parallel()
	for name, input := range map[string]string{
		"fixed-root":            `{"bank":{".patern":{},".pattern":"x"}}`,
		"nested-fixed":          `{"bank":{"deposit":{".patern":{},".pattern":"x"}}}`,
		"non-string":            `{"bank":{"$id":{".patern":{},".pattern":42}}}`,
		"invalid-regex":         `{"bank":{"$id":{".patern":{},".pattern":"["}}}`,
		"root-property":         `{".metadata":{}}`,
		"root-unknown-property": `{".patern":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var chart ChartOfAccounts
			if err := json.Unmarshal([]byte(input), &chart); err == nil {
				t.Fatalf("invalid chart unexpectedly accepted: %s", input)
			}
		})
	}
}

func TestChartUnknownPropertyReplacesExistingChart(t *testing.T) {
	var chart ChartOfAccounts
	if err := json.Unmarshal([]byte(`{"existing":{}}`), &chart); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"new":{".patern":"x"}}`), &chart); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"new":{}}` {
		t.Fatalf("successful parse did not replace the receiver: %s", payload)
	}
}

func TestChartInvalidKnownPropertyDoesNotReplaceExistingChart(t *testing.T) {
	var chart ChartOfAccounts
	if err := json.Unmarshal([]byte(`{"existing":{}}`), &chart); err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"new":{".self":42}}`), &chart); err == nil {
		t.Fatal("invalid known property was accepted")
	}
	after, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("failed parse changed the receiver")
	}
}
