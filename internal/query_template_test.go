package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/query"
)

func TestQueryResolution(t *testing.T) {
	t.Parallel()

	src := `{
	"$and": [
		{"$match": {
			"account": "banks:<iban>:"
		}},
		{"$or": [
			{"$match": {
				"metadata[compliance_type]": "<compliance_type>"
			}},
			{"$exists": {
				"metadata": "<metadata_field>"
			}}
		]}
	]
}`

	params := map[string]string{
		"iban":            "foo",
		"compliance_type": "bar",
		"metadata_field":  "qux",
	}

	expected, err := query.ParseJSON(`{
	"$and": [
		{"$match": {
			"account": "banks:foo:"
		}},
		{"$or": [
			{"$match": {
				"metadata[compliance_type]": "bar"
			}},
			{"$exists": {
				"metadata": "qux"
			}}
		]}
	]
}`)
	require.NoError(t, err)

	resolved, err := ResolveFilterTemplate(json.RawMessage(src), params)
	require.NoError(t, err)

	require.Equal(t, resolved, expected)
}
