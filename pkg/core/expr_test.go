package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRules(t *testing.T) {

	type testCase struct {
		rule             map[string]interface{}
		context          EvalContext
		shouldBeAccepted bool
	}

	var tests = []testCase{
		{
			rule: map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{
						"$gt": []interface{}{
							"$balance", float64(0),
						},
					},
					map[string]interface{}{
						"$eq": []interface{}{
							map[string]interface{}{
								"$meta": "approved",
							},
							"yes",
						},
					},
				},
			},
			context: EvalContext{
				Variables: map[string]interface{}{
					"balance": float64(-10),
				},
				Metadata: Metadata{
					"approved": "yes",
				},
			},
			shouldBeAccepted: true,
		},
		{
			rule: map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{
						"$gte": []interface{}{
							"$balance", float64(0),
						},
					},
					map[string]interface{}{
						"$lte": []interface{}{
							"$balance", float64(0),
						},
					},
				},
			},
			context: EvalContext{
				Variables: map[string]interface{}{
					"balance": float64(-100),
				},
				Metadata: Metadata{},
			},
			shouldBeAccepted: true,
		},
		{
			rule: map[string]interface{}{
				"$lt": []interface{}{
					"$balance", float64(0),
				},
			},
			context: EvalContext{
				Variables: map[string]interface{}{
					"balance": float64(100),
				},
				Metadata: Metadata{},
			},
			shouldBeAccepted: false,
		},
		{
			rule: map[string]interface{}{
				"$lte": []interface{}{
					"$balance", float64(0),
				},
			},
			context: EvalContext{
				Variables: map[string]interface{}{
					"balance": float64(0),
				},
				Metadata: Metadata{},
			},
			shouldBeAccepted: true,
		},
		{
			rule: map[string]interface{}{
				"$and": []interface{}{
					map[string]interface{}{
						"$gt": []interface{}{
							"$balance", float64(0),
						},
					},
					map[string]interface{}{
						"$eq": []interface{}{
							map[string]interface{}{
								"$meta": "approved",
							},
							"yes",
						},
					},
				},
			},
			context: EvalContext{
				Variables: map[string]interface{}{
					"balance": float64(10),
				},
				Metadata: Metadata{
					"approved": "no",
				},
			},
			shouldBeAccepted: false,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			r, err := ParseRuleExpr(test.rule)
			assert.NoError(t, err)
			assert.Equal(t, test.shouldBeAccepted, r.Eval(test.context))
		})
	}

}
