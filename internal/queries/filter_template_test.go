package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/query"
)

func TestFilterTemplateResolution(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		resource        ResourceKind
		varDeclarations map[string]VarSpec
		source          string
		vars            map[string]any
		expectedError   string
		expectedFilter  string
	}

	for _, tc := range []testCase{
		{
			name:     "simple int substitution",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarSpec{
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
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarSpec{
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
			resource: ResourceKindTransaction,
			varDeclarations: map[string]VarSpec{
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
			name:     "substitute in value array",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarSpec{
				"foo": {
					Type:    "string",
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
			name:            "invalid substitution syntax",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarSpec{"minimum_balance": {}},
			source: `{"$gt": {
				"balance[COIN]": "${minimum_balance}000"
			}}`,
			vars:          map[string]any{"minimum_balance": json.Number("42")},
			expectedError: "string or a plain value",
		},
		{
			name:            "invalid field access syntax",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarSpec{"minimum_balance": {}},
			source: `{"$gt": {
				"balance[COIN][THING]": "${minimum_balance}"
			}}`,
			vars:          map[string]any{"minimum_balance": json.Number("42")},
			expectedError: "invalid field name",
		},
		{
			name:            "missing variable",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarSpec{},
			source: `{"$gt": {
				"balance[COIN]": "${doesntexist}"
			}}`,
			vars:          map[string]any{},
			expectedError: "missing variable: doesntexist",
		},
		{
			name:            "unknown field",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarSpec{},
			source: `{"$gt": {
				"doesntexist": "test"
			}}`,
			vars:          map[string]any{},
			expectedError: "unknown field: doesntexist",
		},
		{
			name:            "unexpected indexing",
			resource:        ResourceKindAccount,
			varDeclarations: map[string]VarSpec{},
			source: `{"$gt": {
				"address[COIN]": "test"
			}}`,
			vars:          map[string]any{},
			expectedError: "unexpected field indexing",
		},
		{
			name:     "wrong variable type",
			resource: ResourceKindAccount,
			varDeclarations: map[string]VarSpec{
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
