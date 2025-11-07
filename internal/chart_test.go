package ledger

import (
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"
)

func TestChartOfAccounts(t *testing.T) {
	src := `{
    "banks": {
        "$iban": {
            "_pattern": "*iban_pattern",
            "main": {
                "_rules": {
                    "allowedDestinations": {
                        "thing": true
                    }
                }
            },
            "out": {
                "_metadata": {
                    "key": "value"
                }
            },
            "pending_out": {}
        }
    },
    "users": {
        "$userID": {
            "_self": {},
            "_pattern": "*user_pattern",
            "main": {}
        }
    }
}`

	expected := ChartOfAccounts{
		"banks": {
			VariableSegment: &VariableSegment{
				Label:   "iban",
				Pattern: "*iban_pattern",
				SegmentSchema: SegmentSchema{
					FixedSegments: map[string]SegmentSchema{
						"main": {
							Account: &AccountSchema{
								Rules: AccountRules{
									AllowedDestinations: map[string]interface{}{
										"thing": true,
									},
								},
							},
						},
						"out": {
							Account: &AccountSchema{
								Metadata: map[string]string{
									"key": "value",
								},
							},
						},
						"pending_out": {
							Account: &AccountSchema{},
						},
					},
				},
			},
		},
		"users": {
			VariableSegment: &VariableSegment{
				Label:   "userID",
				Pattern: "*user_pattern",
				SegmentSchema: SegmentSchema{
					Account: &AccountSchema{},
					FixedSegments: map[string]SegmentSchema{
						"main": {
							Account: &AccountSchema{},
						},
					},
				},
			},
		},
	}

	var chart ChartOfAccounts
	err := json.Unmarshal([]byte(src), &chart)
	require.NoError(t, err)

	require.Equal(t, expected, chart)

	value, err := json.MarshalIndent(&chart, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, src, string(value))

}
