package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExpression(t *testing.T) {
	json := `{
	"$and": [
		{
			"$match": {
				"account": "accounts::pending"
			}
		},
		{
			"$or": [
				{
					"$gte": {
						"balance": 1000
					}
				},
				{
					"$match": {
						"metadata[category]": "gold"
					}
				}
			]
		}
	]
}`
	expr, err := ParseJSON(json)
	require.NoError(t, err)

	_, _, err = expr.Build(ContextFn(func(key, operator string, value any) (string, []any, error) {
		return fmt.Sprintf("%s %s ?", key, DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
	}))
	require.NoError(t, err)
}
