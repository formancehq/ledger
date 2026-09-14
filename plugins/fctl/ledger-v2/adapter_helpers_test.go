package ledgerv2

import (
	"reflect"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func TestCollectFlagsAppliesDefaultsAndRejectsUnknownOrRepeatedValues(t *testing.T) {
	t.Parallel()
	command, _ := commandByID("ledger.v2.schemas.list")
	flags, err := collectFlags(command, []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}})
	if err != nil {
		t.Fatalf("collectFlags() error = %v", err)
	}
	if !reflect.DeepEqual(flags["page-size"], []string{"15"}) {
		t.Fatalf("page-size = %#v", flags["page-size"])
	}
	if _, err := collectFlags(command, []sdk.FlagOccurrence{{Name: "unknown", Value: "x"}}); err == nil {
		t.Error("unknown flag accepted")
	}
	if _, err := collectFlags(command, []sdk.FlagOccurrence{{Name: "ledger", Value: "a"}, {Name: "ledger", Value: "b"}}); err == nil {
		t.Error("repeated scalar flag accepted")
	}
	if _, err := collectFlags(command, nil); err == nil {
		t.Error("missing required ledger flag accepted")
	}
}

func TestParseMetadataRejectsMalformedAndDuplicateValues(t *testing.T) {
	if _, err := parseMetadata([]string{"missing-separator"}); err == nil {
		t.Fatal("malformed metadata accepted")
	}
	if _, err := parseMetadata([]string{"key=one", "key=two"}); err == nil {
		t.Fatal("duplicate metadata accepted")
	}
}

func TestNormalizedLogIDAcceptsNumbersAndNumericStrings(t *testing.T) {
	for _, value := range []string{"42", `"0042"`} {
		got, err := normalizedLogID([]byte(value))
		if err != nil || got != "42" {
			t.Fatalf("normalizedLogID(%s) = %q, %v", value, got, err)
		}
	}
	if _, err := normalizedLogID([]byte("-1")); err == nil {
		t.Fatal("negative log id accepted")
	}
}
