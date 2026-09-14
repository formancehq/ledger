package ledger

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestChartUnknownProperties(t *testing.T) {
	t.Parallel()
	keys := []string{".patern", ".metadatas", ".rulez", ".forbiden", ".anything-at-all", "."}
	for _, key := range keys {
		for _, location := range []string{"root-child", "nested-fixed", "variable", "non-account"} {
			t.Run(key+"/"+location, func(t *testing.T) {
				t.Parallel()
				bad := map[string]any{key: map[string]any{"client": true}}
				var input map[string]any
				switch location {
				case "root-child":
					input = map[string]any{"bank": bad}
				case "nested-fixed":
					input = map[string]any{"bank": map[string]any{"deposit": bad}}
				case "variable":
					input = map[string]any{"bank": map[string]any{"$id": bad}}
				case "non-account":
					bad["child"] = map[string]any{}
					input = map[string]any{"bank": bad}
				}
				payload, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				var chart ChartOfAccounts
				err = json.Unmarshal(payload, &chart)
				if err == nil || !strings.Contains(err.Error(), "unknown chart property: "+key) {
					t.Fatalf("expected property-specific error for %s, got %v", payload, err)
				}
			})
		}
	}
}

func TestChartKnownPropertiesRetainMeaning(t *testing.T) {
	t.Parallel()
	input := `{"bank":{".self":{},".rules":{},".metadata":{".business-key":{"default":"x"}},"$id":{".pattern":"^[A-Z]{4}$",".self":{},".metadata":{"region":{"default":"EU"}},"child":{}}}}`
	var chart ChartOfAccounts
	if err := json.Unmarshal([]byte(input), &chart); err != nil {
		t.Fatal(err)
	}
	if chart["bank"].Account == nil {
		t.Fatal(".self account was lost")
	}
	if chart["bank"].VariableSegment == nil || *chart["bank"].VariableSegment.Pattern != "^[A-Z]{4}$" {
		t.Fatal("parent-owned .pattern was lost")
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
		"fixed-root":    `{"bank":{".pattern":"x"}}`,
		"nested-fixed":  `{"bank":{"deposit":{".pattern":"x"}}}`,
		"non-string":    `{"bank":{"$id":{".pattern":42}}}`,
		"invalid-regex": `{"bank":{"$id":{".pattern":"["}}}`,
		"root-property": `{".metadata":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var chart ChartOfAccounts
			if err := json.Unmarshal([]byte(input), &chart); err == nil {
				t.Fatalf("invalid chart unexpectedly accepted: %s", input)
			}
		})
	}
}

func TestChartUnknownPropertyDoesNotReplaceExistingChart(t *testing.T) {
	var chart ChartOfAccounts
	if err := json.Unmarshal([]byte(`{"existing":{}}`), &chart); err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"new":{".patern":"x"}}`), &chart); err == nil {
		t.Fatal("unknown property was accepted")
	}
	after, err := json.Marshal(chart)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("failed parse changed the receiver")
	}
}
