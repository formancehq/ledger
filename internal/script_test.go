package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertScriptV1(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		inputVars map[string]any
		expected  map[string]string
	}

	testCases := []testCase{
		{
			name: "float64 conversion",
			inputVars: map[string]any{
				"amount": map[string]any{
					"asset":  "USD",
					"amount": float64(999999999999999),
				},
			},
			expected: map[string]string{
				"amount": "USD 999999999999999",
			},
		},
		{
			name: "big int conversion",
			inputVars: map[string]any{
				"amount": map[string]any{
					"asset": "USD",
					"amount": func() string {
						ret, _ := big.NewInt(0).SetString("9999999999999999999999999999999999999999", 10)
						return ret.String()
					}(),
				},
			},
			expected: map[string]string{
				"amount": "USD 9999999999999999999999999999999999999999",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script := ScriptV1{
				Script: Script{
					Plain: ``,
				},
				Vars: tc.inputVars,
			}

			converted := script.ToCore()
			require.Equal(t, tc.expected, converted.Vars)
		})
	}
}
