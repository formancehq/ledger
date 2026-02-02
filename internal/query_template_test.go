package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/query"

	"github.com/formancehq/ledger/internal/resources"
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
				Resource:    resources.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]resources.VarSpec{
					"iban": {
						Type: resources.ValueTypeString,
					},
					"minimum_balance": {
						Type: resources.ValueTypeInt,
					},
					"metadata_field": {
						Type:    resources.ValueTypeString,
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
				Resource:    resources.ResourceKindVolume,
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
				Resource:    resources.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]resources.VarSpec{
					"my_bool": {
						Type:    resources.ValueTypeBoolean,
						Default: false,
					},
					"my_int": {
						Type:    resources.ValueTypeInt,
						Default: json.Number("42"),
					},
					"my_string": {
						Type:    resources.ValueTypeString,
						Default: "hello",
					},
					"my_date": {
						Type:    resources.ValueTypeDate,
						Default: "2023-01-01T01:01:01Z",
					},
				},
				Body: json.RawMessage(`{}`),
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
			source: `{
				"description": "invalid params",
				"resource": "volumes",
				"params": {
					"groupLvl": false
				}
			}`,
			expectedError: "cannot unmarshal",
		},
		{
			source: `{
				"description": "wrong variable type",
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
			source: `{
				"description": "undeclared variable",
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

func TestQueryResolution(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		resource        resources.ResourceKind
		varDeclarations map[string]resources.VarSpec
		source          string
		vars            map[string]any
		expectedError   string
		expectedFilter  string
	}

	for _, tc := range []testCase{
		{
			name:     "simple int substitution",
			resource: resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{
				"minimum_balance": {},
			},
			source: `{
				"$gt": {
					"balance[COIN]": "${minimum_balance}"
				}
			}`,
			vars: map[string]any{
				"minimum_balance": json.Number("42"),
			},
			expectedFilter: `{
				"$gt": {
					"balance[COIN]": 42
				}
			}`,
		},
		{
			name:     "complex",
			resource: resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{
				"iban":            {},
				"minimum_balance": {},
				"metadata_field": {
					Default: "qux",
				},
			},
			source: `{
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
			}`,
			vars: map[string]any{
				"iban":            "foo",
				"minimum_balance": json.Number("1000"),
			},
			expectedFilter: `{
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
			}`,
		},
		{
			name:     "different types",
			resource: resources.ResourceKindTransaction,
			varDeclarations: map[string]resources.VarSpec{
				"my_bool":   {},
				"my_int":    {},
				"my_string": {},
				"my_date":   {},
			},
			source: `{
				"$and": [
					{"$match": {"reverted": "${my_bool}"}},
					{"$match": {"account": "prefix:${my_string}:suffix"}},
					{"$match": {"timestamp": "${my_date}"}},
					{"$match": {"id": "${my_int}"}}
				]
			}`,
			vars: map[string]any{
				"my_bool":   false,
				"my_int":    json.Number("1234"),
				"my_string": "foobarbazqux",
				"my_date":   "2023-01-01T01:01:01Z",
			},
			expectedFilter: `{
				"$and": [
					{"$match": {"reverted": false}},
					{"$match": {"account": "prefix:foobarbazqux:suffix"}},
					{"$match": {"timestamp": "2023-01-01T01:01:01Z"}},
					{"$match": {"id": 1234}}
				]
			}`,
		},
		{
			name:            "invalid substitution syntax",
			resource:        resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{"minimum_balance": {}},
			source: `{"$gt": {
				"balance[COIN]": "${minimum_balance}000"
			}}`,
			vars:          map[string]any{"minimum_balance": json.Number("42")},
			expectedError: "string or a plain value",
		},
		{
			// should be elsewhere
			name:            "invalid field access syntax",
			resource:        resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{"minimum_balance": {}},
			source: `{"$gt": {
				"balance[COIN][THING]": "${minimum_balance}"
			}}`,
			vars:          map[string]any{"minimum_balance": json.Number("42")},
			expectedError: "invalid field name",
		},
		{
			name:            "missing variable",
			resource:        resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{},
			source: `{"$gt": {
				"balance[COIN]": "${doesntexist}"
			}}`,
			vars:          map[string]any{},
			expectedError: "missing variable: doesntexist",
		},
		{
			name:            "unknown field",
			resource:        resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{},
			source: `{"$gt": {
				"doesntexist": "test"
			}}`,
			vars:          map[string]any{},
			expectedError: "unknown field: doesntexist",
		},
		{
			name:     "wrong variable type",
			resource: resources.ResourceKindAccount,
			varDeclarations: map[string]resources.VarSpec{
				"wrongtype": {
					Type:    "string",
					Default: "test",
				},
			},
			source: `{"$gt": {
				"balance[COIN]": "${wrongtype}"
			}}`,
			vars:          map[string]any{},
			expectedError: "cannot use variable `wrongtype` as type `Number`",
		},
	} {
		resolved, err := resources.ResolveFilterTemplate(tc.resource, json.RawMessage(tc.source), tc.varDeclarations, tc.vars)

		if tc.expectedError == "" {
			require.NoError(t, err, tc.name)

			expected, err := query.ParseJSON(tc.expectedFilter)
			require.NoError(t, err, tc.name)
			require.Equal(t, expected, resolved, tc.name)
		} else {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		}
	}
}
