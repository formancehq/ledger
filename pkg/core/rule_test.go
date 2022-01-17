package core

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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
							"$balance", 0,
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
					"balance": -10,
				},
				Metadata: map[string]json.RawMessage{
					"approved": json.RawMessage("yes"),
				},
			},
			shouldBeAccepted: true,
		},
		{
			rule: map[string]interface{}{
				"$and": []interface{}{
					map[string]interface{}{
						"$gt": []interface{}{
							"$balance", 0,
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
					"balance": 10,
				},
				Metadata: map[string]json.RawMessage{
					"approved": json.RawMessage("no"),
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
