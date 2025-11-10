package ledger

import (
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"
)

func expectValidChart(t *testing.T, source string, expected ChartOfAccounts) {
	var chart ChartOfAccounts
	err := json.Unmarshal([]byte(source), &chart)
	require.NoError(t, err)

	require.Equal(t, expected, chart)

	value, err := json.MarshalIndent(&chart, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, source, string(value))
}

func expectInvalidChart(t *testing.T, source string, expectedError string) {
	var chart ChartOfAccounts
	err := json.Unmarshal([]byte(source), &chart)

	require.ErrorContains(t, err, expectedError)
}

func TestChartOfAccounts(t *testing.T) {
	src := `{
    "banks": {
        "$iban": {
            "_pattern": "*iban_pattern",
            "main": {
                "_rules": {
                    "allowedDestinations": ["thing"]
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
									AllowedDestinations: []string{"thing"},
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

	expectValidChart(t, src, expected)
}

func TestInvalidFixedSegment(t *testing.T) {
	src := `{
		"banks": {
			"main:40": {}
		}
	}`
	expectInvalidChart(t, src, "invalid address segment: main:40")
}

func TestInvalidSubsegment(t *testing.T) {
	src := `{
		"banks": {
			"main": 42
		}
	}`
	expectInvalidChart(t, src, "invalid subsegment")
}

func TestInvalidPatternOnFixed(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				"_pattern": "[0-9]{3}"
			}
		}
	}`
	expectInvalidChart(t, src, "cannot have a pattern on a fixed segment")
}

func TestInvalidMultipleVariableSegments(t *testing.T) {
	src := `{
		"users": {
			"$userID": {
				"_pattern": "[0-9]{3}"
			},
			"$otherID": {
				"_pattern": "[0-9]{4}"
			}
		}
	}`
	expectInvalidChart(t, src, "invalid subsegments: cannot have two variable segments with the same prefix")
}

func TestInvalidVariableSegmentWithoutPattern(t *testing.T) {
	src := `{
		"users": {
			"$userID": {
				"_metadata": {
					"key": "value"
				}
			}
		}
	}`
	expectInvalidChart(t, src, "cannot have a variable segment without a pattern")
}

func TestInvalidMetadata(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				"_metadata": 42
			}
		}
	}`
	expectInvalidChart(t, src, "invalid subsegment")
}

func TestInvalidRules(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				"_rules": 42
			}
		}
	}`
	expectInvalidChart(t, src, "invalid subsegment")
}

func TestChartValidation(t *testing.T) {
	chart := ChartOfAccounts{
		"bank": {
			VariableSegment: &VariableSegment{
				Label:   "bankID",
				Pattern: "[0-9]{3}",
				SegmentSchema: SegmentSchema{
					Account: &AccountSchema{},
				},
			},
			Account: &AccountSchema{},
		},
		"users": {
			VariableSegment: &VariableSegment{
				Label:   "userID",
				Pattern: "[0-9]{3}",
				SegmentSchema: SegmentSchema{
					FixedSegments: map[string]SegmentSchema{
						"main": {
							Account: &AccountSchema{},
						},
					},
				},
			},
		},
	}

	_, err := chart.FindAccountSchema("world")
	require.NoError(t, err)

	_, err = chart.FindAccountSchema("bank")
	require.NoError(t, err)

	_, err = chart.FindAccountSchema("bank:012")
	require.NoError(t, err)

	_, err = chart.FindAccountSchema("users:001:main")
	require.NoError(t, err)

	_, err = chart.FindAccountSchema("users:abc:main")
	require.ErrorIs(t, err, ErrInvalidAccount{})

	_, err = chart.FindAccountSchema("users:001")
	require.ErrorIs(t, err, ErrInvalidAccount{})

	_, err = chart.FindAccountSchema("users")
	require.ErrorIs(t, err, ErrInvalidAccount{})
}
