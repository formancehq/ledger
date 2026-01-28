package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/query"

	"github.com/formancehq/ledger/internal/resources"
)

func TestQueryResolution(t *testing.T) {

	t.Parallel()

	varDeclarations := map[string]VarSpec{
		"iban":            {},
		"minimum_balance": {},
		"metadata_field":  {},
	}

	src := `{
	"$and": [
		{"$match": {
			"address": "banks:<iban>:"
		}},
		{"$or": [
			{"$gt": {
				"balance[COIN]": "<minimum_balance>"
			}},
			{"$exists": {
				"metadata": "<metadata_field>"
			}}
		]}
	]
}`

	vars := map[string]any{
		"iban":            "foo",
		"minimum_balance": json.Number("1000"),
		"metadata_field":  "qux",
	}

	expected, err := query.ParseJSON(`{
	"$and": [
		{"$match": {
			"address": "banks:foo:"
		}},
		{"$or": [
			{"$gt": {
				"balance[COIN]": 1000
			}},
			{"$exists": {
				"metadata": "qux"
			}}
		]}
	]
}`)
	require.NoError(t, err)

	resolved, err := ResolveFilterTemplate(resources.ResourceKindAccount, json.RawMessage(src), varDeclarations, vars)
	require.NoError(t, err)

	require.Equal(t, expected, resolved)
}

func TestQueryResolveInt(t *testing.T) {
	t.Parallel()

	src := `{
		"$gt": {
			"balance[COIN]": "<minimum_balance>"
		}
	}`

	vars := map[string]any{
		"minimum_balance": json.Number("1000"),
	}

	expected, err := query.ParseJSON(`{
		"$gt": {
			"balance[COIN]": 1000
		}
	}`)
	require.NoError(t, err)

	resolved, err := ResolveFilterTemplate(resources.ResourceKindAccount, json.RawMessage(src), map[string]VarSpec{}, vars)
	require.NoError(t, err)

	require.Equal(t, expected, resolved)

}
