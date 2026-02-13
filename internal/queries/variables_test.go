package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVariableDeclarationMarshal(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		source            string
		expectedVars      map[string]VarDecl
		expectedRoundtrip bool
		expectedError     string
	}

	for _, tc := range []testCase{
		{
			name: "invalid json.Number integer",
			source: `{
				"aaa": "string",
				"bbb": "boolean",
				"ccc": "int",
				"ddd": "date",
				"eee": {
					"type": "int",
					"default": 42
				}
			}`,
			expectedVars: map[string]VarDecl{
				"aaa": {Type: NewTypeString()},
				"bbb": {Type: NewTypeBoolean()},
				"ccc": {Type: NewTypeNumeric()},
				"ddd": {Type: NewTypeDate()},
				"eee": {Type: NewTypeNumeric(), Default: json.Number("42")},
			},
		},
		{
			name: "invalid json.Number integer",
			source: `{
				"foo": {
					"type": "int",
					"default": 42
				}
			}`,
			expectedVars: map[string]VarDecl{
				"foo": {Type: NewTypeNumeric(), Default: json.Number("42")},
			},
			expectedRoundtrip: true,
		},
	} {
		var actualVars map[string]VarDecl
		err := json.Unmarshal([]byte(tc.source), &actualVars)
		if tc.expectedError != "" {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		} else {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedVars, actualVars, tc.name)
			if tc.expectedRoundtrip {
				marshalled, err := json.Marshal(actualVars)
				require.NoError(t, err, tc.name)
				require.JSONEq(t, tc.source, string(marshalled), tc.name)
			}
		}

	}
}

func TestVariableValidaton(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		vars          map[string]VarDecl
		expectedError string
	}

	for _, tc := range []testCase{
		{
			name: "invalid json.Number integer",
			vars: map[string]VarDecl{
				"foo": {Type: NewTypeNumeric(), Default: json.Number("133.7")},
			},
			expectedError: "invalid value `133.7` for type `int`",
		},
		{
			name: "invalid float64 integer",
			vars: map[string]VarDecl{
				"foo": {Type: NewTypeNumeric(), Default: 133.7},
			},
			expectedError: "invalid value `133.7` for type `int`",
		},
	} {
		err := ValidateVarDeclarations(tc.vars)

		if tc.expectedError != "" {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		} else {
			require.NoError(t, err, tc.name)
		}
	}
}
