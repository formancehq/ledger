package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/query"
)

func TestFilterTemplateValidation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		resource        ResourceKind
		varDeclarations map[string]VarDecl
		source          string
		expectedError   string
	}

	for _, tc := range []testCase{
		{
			name:            "invalid substitution syntax",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{"minimum_balance": {Type: NewTypeNumeric()}},
			source: `{"$gt": {
				"balance[COIN]": "${minimum_balance}000"
			}}`,
			expectedError: "string or a plain value",
		},
		{
			name:            "missing variable",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$gt": {
				"balance[COIN]": "${doesntexist}"
			}}`,
			expectedError: "variable `doesntexist` is not declared",
		},
		{
			name:            "missing variable in interpolation",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$match": {
				"address": "${doesntexist}:foo"
			}}`,
			expectedError: "variable `doesntexist` is not declared",
		},
		{
			name:     "invalid field access syntax",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"minimum_balance": {Type: NewTypeNumeric(), Default: 42},
			},
			source: `{"$gt": {
				"balance[COIN][THING]": "${minimum_balance}"
			}}`,
			expectedError: "invalid field name",
		},
		{
			name:            "unknown field",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$gt": {
				"doesntexist": "test"
			}}`,
			expectedError: "unknown field: doesntexist",
		},
		{
			name:            "unexpected indexing",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$gt": {
				"address[COIN]": "test"
			}}`,
			expectedError: "unexpected field indexing",
		},
		{
			name:            "missing indexing",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$gt": {
				"balance": 42
			}}`,
			expectedError: "invalid value `42` for type `map[string]int`",
		},
		{
			name:     "wrong variable type",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"wrongtype": {
					Type:    NewTypeString(),
					Default: "test",
				},
			},
			source: `{"$gt": {
				"balance[COIN]": "${wrongtype}"
			}}`,
			expectedError: "cannot use variable `wrongtype` as type `int`",
		},
		{
			name:            "$in with plain value",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$in": {
				"address": "foo"
			}}`,
			expectedError: "expected array, got `string`",
		},
		{
			name:            "$exists with a non-map field",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{"$exists": {
				"address": "foo"
			}}`,
			expectedError: "$exists can only be called on a map field",
		},
	} {
		err := ValidateFilterBody(tc.resource, json.RawMessage(tc.source), tc.varDeclarations)

		if tc.expectedError != "" {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		} else {
			require.NoError(t, err, tc.name)
		}
	}
}

func TestFilterTemplateResolution(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		resource        ResourceKind
		varDeclarations map[string]VarDecl
		source          string
		vars            map[string]any
		expectedError   string
		expectedFilter  string
	}

	for _, tc := range []testCase{
		{
			name:            "trivial case",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source:          `null`,
			vars:            map[string]any{},
			expectedFilter:  `null`,
		},
		{
			name:            "trivial case",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarDecl{},
			source: `{
				"$gt": {
					"balance[COIN]": 42
				}
			}`,
			vars: map[string]any{},
			expectedFilter: `{
				"$gt": {
					"balance[COIN]": 42
				}
			}`,
		},
		{
			name:     "simple int substitution",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"minimum_balance": {Type: NewTypeNumeric()},
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
			name:     "simple int substitution with float64",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"minimum_balance": {Type: NewTypeNumeric()},
			},
			source: `{
				"$gt": {
					"balance[COIN]": "${minimum_balance}"
				}
			}`,
			vars: map[string]any{
				"minimum_balance": float64(42.0),
			},
			expectedFilter: `{
				"$gt": {
					"balance[COIN]": 42
				}
			}`,
		},
		{
			name:     "complex",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"iban":            {Type: NewTypeString()},
				"minimum_balance": {Type: NewTypeNumeric()},
				"metadata_field": {
					Type:    NewTypeString(),
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
			resource: ResourceKindTransaction,
			varDeclarations: map[string]VarDecl{
				"my_boolean": {Type: NewTypeBoolean()},
				"my_int":     {Type: NewTypeNumeric()},
				"my_string":  {Type: NewTypeString()},
				"my_date":    {Type: NewTypeDate()},
			},
			source: `{
				"$and": [
					{"$match": {"reverted": "${my_boolean}"}},
					{"$match": {"account": "prefix:${my_string}:suffix"}},
					{"$match": {"timestamp": "${my_date}"}},
					{"$match": {"id": "${my_int}"}}
				]
			}`,
			vars: map[string]any{
				"my_boolean": false,
				"my_int":     json.Number("1234"),
				"my_string":  "foobarbazqux",
				"my_date":    "2023-01-01T01:01:01Z",
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
			name:     "substitute in value array",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"foo": {
					Type:    NewTypeString(),
					Default: "foovalue",
				},
			},
			source: `{
				"$in": {
					"address": ["${foo}", "barvalue"]
				}
			}`,
			vars: map[string]any{},
			expectedFilter: `{
				"$in": {
					"address": ["foovalue", "barvalue"]
				}
			}`,
		},
		{
			name:     "variable not provided",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"foo": {Type: NewTypeNumeric()},
			},
			source: `{"$gt": {
				"balance[COIN]": "${foo}"
			}}`,
			vars:          map[string]any{},
			expectedError: "missing variable: `foo`",
		},
		{
			name:     "wrong provided variable type",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarDecl{
				"foo": {Type: NewTypeNumeric()},
			},
			source: `{"$match": {
				"address": "${foo}:nope"
			}}`,
			vars: map[string]any{
				"foo": "nope",
			},
			expectedError: "invalid value `nope` for type `int`",
		},
	} {
		err := ValidateFilterBody(tc.resource, json.RawMessage(tc.source), tc.varDeclarations)
		require.NoError(t, err, tc.name)

		resolved, err := ResolveFilterTemplate(tc.resource, json.RawMessage(tc.source), tc.varDeclarations, tc.vars)
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
