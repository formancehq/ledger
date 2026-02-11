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
					"iban": "string"
				},
				"body": { "$match": { "address": "banks:${iban}:" } }
			}`,
			expectedTemplate: QueryTemplate{
				Description: "complex & valid",
				Resource:    queries.ResourceKindAccount,
				Params:      nil,
				Vars: map[string]queries.VarDecl{
					"iban": {
						Type: queries.NewTypeString(),
					},
				},
				Body: json.RawMessage(`{ "$match": { "address": "banks:${iban}:" } }`),
			},
		},
		{
			name: "params",
			source: `{
				"description": "complex params",
				"resource": "volumes",
				"params": {"pageSize": 42, "groupLvl": 2}
			}`,
			expectedTemplate: QueryTemplate{
				Description: "complex params",
				Resource:    queries.ResourceKindVolume,
				Params:      json.RawMessage(`{"pageSize": 42, "groupLvl": 2}`),
				Vars:        nil,
				Body:        nil,
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
				Vars: map[string]queries.VarDecl{
					"foo": {
						Type: queries.NewTypeString(),
					},
					"bar": {
						Type: queries.NewTypeString(),
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
					"groupBy": false
				}
			}`,
			expectedError: "cannot unmarshal",
		},
		{
			name: "filter validation error",
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
			name: "invalid sort column",
			source: `{
				"resource": "accounts",
				"params": {
					"sort": "balance:asc"
				}
			}`,
			expectedError: "invalid sort column",
		},
		{
			name: "invalid sort column 2",
			source: `{
				"resource": "accounts",
				"params": {
					"sort": ":asc"
				}
			}`,
			expectedError: "invalid sort column",
		},
	} {
		var template QueryTemplate
		err := unmarshalWithNumber([]byte(tc.source), &template)
		require.NoError(t, err)

		err = template.Validate()
		if tc.expectedError == "" {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedTemplate, template, tc.name)
		} else {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		}
	}
}
