package ledger

import (
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"
)

func expectInvalidChart(t *testing.T, source string, expectedError string) {
	var chart ChartOfAccounts
	err := json.Unmarshal([]byte(source), &chart)

	require.ErrorContains(t, err, expectedError)
}

func TestChartOfAccounts(t *testing.T) {
	source := `{
    "banks": {
        "$iban": {
            "_pattern": "*iban_pattern",
            "main": {
                "_rules": {}
            },
            "out": {
                "_metadata": {
                    "key": "value"
                },
                "_rules": {}
            },
            "pending_out": {
                "_rules": {}
            }
        }
    },
    "users": {
        "$userID": {
            "_self": {},
            "_pattern": "*user_pattern",
            "_rules": {},
            "main": {
                "_rules": {}
            }
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
								Rules: AccountRules{},
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
	err := json.Unmarshal([]byte(source), &chart)
	require.NoError(t, err)

	require.Equal(t, expected, chart)

	value, err := json.MarshalIndent(&chart, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, source, string(value))
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
	expectInvalidChart(t, src, "invalid segment")
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
	expectInvalidChart(t, src, "cannot have two variable segments with the same prefix")
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
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidRules(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				"_rules": 42
			}
		}
	}`
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidAccountSchema(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				"_self": {
					"_rules": {}
				}
			}
		}
	}`
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidRootSegment(t *testing.T) {
	src := `{ "_banks": { "_self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")

	src = `{ "$banks": { "pattern": "[0-9]+", "_self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")

	src = `{ "abc:abc": { "_self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")
}

func testChart() ChartOfAccounts {
	return ChartOfAccounts{
		"bank": {
			VariableSegment: &VariableSegment{
				Label:   "bankID",
				Pattern: "[0-9]{3}",
				SegmentSchema: SegmentSchema{
					Account: &AccountSchema{
						Rules: AccountRules{},
					},
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
}

func TestAccountValidation(t *testing.T) {
	chart := testChart()

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

func TestPostingValidation(t *testing.T) {
	chart := testChart()

	err := chart.ValidatePosting(Posting{
		Source:      "bank:012",
		Destination: "users:012:main",
	})
	require.NoError(t, err)

	err = chart.ValidatePosting(Posting{
		Source:      "bank:invalid",
		Destination: "users:001:main",
	})
	require.ErrorContains(t, err, "not allowed by the chart")

	err = chart.ValidatePosting(Posting{
		Source:      "bank:012",
		Destination: "users:invalid:main",
	})
	require.ErrorContains(t, err, "not allowed by the chart")
}
