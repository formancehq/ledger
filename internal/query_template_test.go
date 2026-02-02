package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/queries"
)

func TestQueryTemplateValidation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name             string
		source           string
		expectedTemplate QueryTemplate
		expectedError    string
	}

	for _, tc := range []testCase{
		{
			name: "complex & valid",
			source: `{
				"description": "complex & valid",
				"resource": "accounts",
				"vars": {
					"iban":            "string",
					"minimum_balance": "int",
					"metadata_field": {
						"type": "string",
						"default": "qux"
					}
				},
				"body": {
	"$and": [
		{"$match": {
			"address": "banks:${iban}:"
		}},
		{"$or": [
			{"$gt": {
				"balance[COIN]": "${minimum_balance}"
			}},
			{"$exists": {
				"metadata": "${metadata_field}"
			}}
		]}
	]
}}`,
			expectedTemplate: QueryTemplate{
				Description: "complex & valid",
				Resource:    queries.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]queries.VarSpec{
					"iban": {
						Type: queries.ValueTypeString,
					},
					"minimum_balance": {
						Type: queries.ValueTypeInt,
					},
					"metadata_field": {
						Type:    queries.ValueTypeString,
						Default: "qux",
					},
				},
				Body: json.RawMessage(`{
	"$and": [
		{"$match": {
			"address": "banks:${iban}:"
		}},
		{"$or": [
			{"$gt": {
				"balance[COIN]": "${minimum_balance}"
			}},
			{"$exists": {
				"metadata": "${metadata_field}"
			}}
		]}
	]
}`),
			},
		},
		{
			name: "params",
			source: `{
				"description": "complex params",
				"resource": "volumes",
				"params": {"groupLvl": 2}
			}`,
			expectedTemplate: QueryTemplate{
				Description: "complex params",
				Resource:    queries.ResourceKindVolume,
				Params:      json.RawMessage(`{"groupLvl": 2}`),
				Vars:        nil,
				Body:        nil,
			},
		},
		{
			name: "all types",
			source: `{
				"description": "all types",
				"resource": "accounts",
				"vars": {
					"my_bool":   {
						"type": "bool",
						"default": false
					},
					"my_int":    {
						"type": "int",
						"default": 42
					},
					"my_string": {
						"type": "string",
						"default": "hello"
					},
					"my_date":   {
						"type": "date",
						"default": "2023-01-01T01:01:01Z"
					}
				},
				"body": {}
			}`,
			expectedTemplate: QueryTemplate{
				Description: "all types",
				Resource:    queries.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]queries.VarSpec{
					"my_bool": {
						Type:    queries.ValueTypeBoolean,
						Default: false,
					},
					"my_int": {
						Type:    queries.ValueTypeInt,
						Default: json.Number("42"),
					},
					"my_string": {
						Type:    queries.ValueTypeString,
						Default: "hello",
					},
					"my_date": {
						Type:    queries.ValueTypeDate,
						Default: "2023-01-01T01:01:01Z",
					},
				},
				Body: json.RawMessage(`{}`),
			},
		},
		{
			source: `{
				"description": "$in filter",
				"resource": "accounts",
				"vars": {
					"foo": "string",
					"bar": "string"
				},
				"body": {
					"$in": {
						"metadata[foo]": ["${foo}", "${bar}"]
					}
				}
			}`,
			expectedTemplate: QueryTemplate{
				Description: "$in filter",
				Resource:    queries.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]queries.VarSpec{
					"foo": {
						Type: queries.ValueTypeString,
					},
					"bar": {
						Type: queries.ValueTypeString,
					},
				},
				Body: json.RawMessage(`{
					"$in": {
						"metadata[foo]": ["${foo}", "${bar}"]
					}
				}`),
			},
		},
		{
			source: `{
				"description": "unknown resource kind",
				"resource": "doesntexist"
			}`,
			expectedError: "unknown resource kind",
		},
		{
			source: `{
				"description": "invalid variable type",
				"resource": "transactions",
				"vars": {
					"my_bool": "doesntexist"
				}
			}`,
			expectedError: "invalid type",
		},
		{
			source: `{
				"description": "invalid default",
				"resource": "transactions",
				"vars": {
					"my_bool": {
						"type": "bool",
						"default": "wrongtype"
					}
				}
			}`,
			expectedError: "invalid default",
		},
		{
			name: "invalid common params",
			source: `{
				"resource": "volumes",
				"params": {
					"sort": {}
				}
			}`,
			expectedError: "cannot unmarshal",
		},
		{
			name: "invalid resource-specific params",
			source: `{
				"resource": "volumes",
				"params": {
					"groupLvl": false
				}
			}`,
			expectedError: "cannot unmarshal",
		},
		{
			name: "wrong variable type",
			source: `{
				"resource": "accounts",
				"vars": {
					"foo": "string"
				},
				"body": {
					"$match": {
						"balance[COIN]": "${foo}"
					}
				}
			}`,
			expectedError: "cannot use variable",
		},
		{
			name: "undeclared variable",
			source: `{
				"resource": "accounts",
				"vars": {},
				"body": {
					"$match": {
						"balance[COIN]": "${foo}"
					}
				}
			}`,
			expectedError: "variable `foo` is not declared",
		},
	} {
		var template QueryTemplate
		err := unmarshalWithNumber([]byte(tc.source), &template)
		require.NoError(t, err)

		err = template.Validate()
		if tc.expectedError == "" {
			require.Equal(t, tc.expectedTemplate, template, tc.name)
		} else {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		}
	}
}
