package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExpression(t *testing.T) {
	json := `{
	"$not": {
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
	}
}`
	expr, err := ParseJSON(json)
	require.NoError(t, err)

	q, args, err := expr.Build(ContextFn(func(key, operator string, value any) (string, []any, error) {
		return fmt.Sprintf("%s %s ?", key, DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
	}))
	require.NoError(t, err)
	require.Equal(t, "not ((account = ?) and ((balance >= ?) or (metadata[category] = ?)))", q)
	require.Equal(t, []any{
		"accounts::pending",
		float64(1000),
		"gold",
	}, args)
}
