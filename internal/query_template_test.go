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
				"params": {"pageSize": 42, "groupBy": 2}
			}`,
			expectedTemplate: QueryTemplate{
				Description: "complex params",
				Resource:    queries.ResourceKindVolume,
				Params:      json.RawMessage(`{"pageSize": 42, "groupBy": 2}`),
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
			name: "params",
			source: `{
				"description": "complex params",
				"resource": "accounts",
				"params": {"nope": 2}
			}`,
			expectedError: "invalid params: unknown field: `nope`",
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

func TestQueryTemplateParamsOverwriteKeepsUnsetFields(t *testing.T) {
	t.Parallel()

	params, err := QueryTemplateParams[any]{
		PageSize:   15,
		SortColumn: "id",
	}.Overwrite(
		json.RawMessage(`{"endTime": "2024-01-01T00:00:00Z", "startTime": "2023-01-01T00:00:00Z", "expand": ["volumes"], "pageSize": 5}`),
		json.RawMessage(`{"sort": "id:asc"}`),
	)
	require.NoError(t, err)

	require.Equal(t, uint(5), params.PageSize)
	require.NotNil(t, params.PIT)
	require.Equal(t, "2024-01-01T00:00:00Z", params.PIT.Format("2006-01-02T15:04:05Z07:00"))
	require.NotNil(t, params.OOT)
	require.Equal(t, "2023-01-01T00:00:00Z", params.OOT.Format("2006-01-02T15:04:05Z07:00"))
	require.Equal(t, []string{"volumes"}, params.Expand)
	require.Equal(t, "id", params.SortColumn)
	require.NotNil(t, params.SortOrder)

	// A template without pageSize keeps the configured default.
	params, err = QueryTemplateParams[any]{PageSize: 15}.Overwrite(json.RawMessage(`{"sort": "id:asc"}`))
	require.NoError(t, err)
	require.Equal(t, uint(15), params.PageSize)
}
