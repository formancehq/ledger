package ledger

import (
	"encoding/json"
	"fmt"

	"testing"

	"github.com/formancehq/go-libs/v3/pointer"
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
		{
			Fixed: pointer.For("banks"),
			Segments: []SegmentSchema{
				{
					Label:   pointer.For("iban"),
					Pattern: pointer.For("*iban_pattern"),
					Segments: []SegmentSchema{
						{
							Fixed: pointer.For("main"),
							Account: &AccountSchema{
								Rules: AccountRules{
									AllowedDestinations: map[string]interface{}{
										"thing": true,
									},
								},
							},
						},
						{
							Fixed: pointer.For("out"),
							Account: &AccountSchema{
								Metadata: map[string]string{
									"key": "value",
								},
							},
						},
						{
							Fixed:   pointer.For("pending_out"),
							Account: &AccountSchema{},
						},
					},
				},
			},
		},
		{
			Fixed: pointer.For("users"),
			Segments: []SegmentSchema{
				{
					Label:   pointer.For("userID"),
					Pattern: pointer.For("*user_pattern"),
					Account: &AccountSchema{},
					Segments: []SegmentSchema{
						{
							Fixed:   pointer.For("main"),
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

	require.Equal(t, expected[0], chart[0])

	value, err := json.MarshalIndent(&chart, "", "  ")
	require.NoError(t, err)
	fmt.Printf("%v\n", string(value))

}
