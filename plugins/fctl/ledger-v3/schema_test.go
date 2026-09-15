package ledgerv3

import (
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func TestBuildInputSchemaDerivesClosedObjectFromGrammar(t *testing.T) {
	t.Parallel()

	got := string(buildInputSchema(
		[]sdk.Argument{{Name: "ledger", Usage: "Ledger name", Type: sdk.ArgumentString, Required: true}},
		[]sdk.Flag{{Name: "limit", Usage: "Page size", Type: sdk.FlagInt32, HasDefault: true, DefaultValue: "50"}},
	))

	want := `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object",` +
		`"properties":{"ledger":{"type":"string"},"limit":{"type":"integer","default":50}},` +
		`"required":["ledger"],"additionalProperties":false}`
	if got != want {
		t.Fatalf("buildInputSchema =\n%s\nwant\n%s", got, want)
	}
}
