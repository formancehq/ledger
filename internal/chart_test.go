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
            ".pattern": "[0-9]{10}",
            "main": {
                ".rules": {}
            },
            "out": {
                ".metadata": {
                    "key": "value"
                },
                ".rules": {}
            },
            "pending_out": {
                ".rules": {}
            }
        }
    },
    "users": {
        "$userID": {
            ".self": {},
            ".pattern": "[0-9]{5}",
            ".rules": {},
            "main": {
                ".rules": {}
            }
        }
    }
}`
	expected := ChartOfAccounts{
		"banks": {
			VariableSegment: &ChartVariableSegment{
				Label:   "iban",
				Pattern: "[0-9]{10}",
				ChartSegment: ChartSegment{
					FixedSegments: map[string]ChartSegment{
						"main": {
							Account: &ChartAccount{
								Rules: ChartAccountRules{},
							},
						},
						"out": {
							Account: &ChartAccount{
								Metadata: map[string]string{
									"key": "value",
								},
							},
						},
						"pending_out": {
							Account: &ChartAccount{},
						},
					},
				},
			},
		},
		"users": {
			VariableSegment: &ChartVariableSegment{
				Label:   "userID",
				Pattern: "[0-9]{5}",
				ChartSegment: ChartSegment{
					Account: &ChartAccount{},
					FixedSegments: map[string]ChartSegment{
						"main": {
							Account: &ChartAccount{},
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
				".pattern": "[0-9]{3}"
			}
		}
	}`
	expectInvalidChart(t, src, "cannot have a pattern on a fixed segment")
}

func TestInvalidMultipleVariableSegments(t *testing.T) {
	src := `{
		"users": {
			"$userID": {
				".pattern": "[0-9]{3}"
			},
			"$otherID": {
				".pattern": "[0-9]{4}"
			}
		}
	}`
	expectInvalidChart(t, src, "cannot have two variable segments with the same prefix")
}

func TestInvalidVariableSegmentWithoutPattern(t *testing.T) {
	src := `{
		"users": {
			"$userID": {
				".metadata": {
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
				"metadata": 42
			}
		}
	}`
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidRules(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				".rules": 42
			}
		}
	}`
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidAccountSchema(t *testing.T) {
	src := `{
		"banks": {
			"main": {
				".self": {
					".rules": {}
				}
			}
		}
	}`
	expectInvalidChart(t, src, "invalid segment")
}

func TestInvalidRootSegment(t *testing.T) {
	src := `{ ".banks": { ".self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")

	src = `{ "$banks": { ".pattern": "[0-9]+", ".self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")

	src = `{ "abc:abc": { ".self": {} } }`
	expectInvalidChart(t, src, "invalid segment name")
}

func TestInvalidPatternType(t *testing.T) {
	src := `{ "banks": { "$bankID": { ".pattern": 42 } } }`
	expectInvalidChart(t, src, "pattern must be a string")
}

func TestInvalidPatternRegex(t *testing.T) {
	src := `{ "banks": { "$bankID": { ".pattern": "[[" } } }`
	expectInvalidChart(t, src, "invalid pattern regex")
}

func TestInvalidSelf(t *testing.T) {
	src := `{ "foo": {
		".self": 42,
		"bar": { "baz": {} }
} }`
	expectInvalidChart(t, src, ".self must be an empty object")

	src = `{ "foo": {
	".self": {
		"key": "value"
	},
	"bar": { "baz": {} }
} }`
	expectInvalidChart(t, src, ".self must be an empty object")
}

func testChart() ChartOfAccounts {
	return ChartOfAccounts{
		"bank": {
			VariableSegment: &ChartVariableSegment{
				Label:   "bankID",
				Pattern: "[0-9]{3}",
				ChartSegment: ChartSegment{
					Account: &ChartAccount{
						Rules: ChartAccountRules{},
					},
				},
			},
			Account: &ChartAccount{},
		},
		"users": {
			VariableSegment: &ChartVariableSegment{
				Label:   "userID",
				Pattern: "[0-9]{3}",
				ChartSegment: ChartSegment{
					FixedSegments: map[string]ChartSegment{
						"main": {
							Account: &ChartAccount{},
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
