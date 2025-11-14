package ledger

import (
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"
)

func TestChartValidation(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		source        string
		expectedError string
		expectedChart ChartOfAccounts
	}

	for _, tc := range []testCase{
		{
			name: "valid chart",
			source: `{
    "banks": {
        "$iban": {
            ".pattern": "^[0-9]{10}$",
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
            ".pattern": "^[0-9]{5}$",
            ".rules": {},
            "main": {
                ".rules": {}
            }
        }
    }
}`,
			expectedChart: ChartOfAccounts{
				"banks": {
					VariableSegment: &ChartVariableSegment{
						Label:   "iban",
						Pattern: "^[0-9]{10}$",
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
						Pattern: "^[0-9]{5}$",
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
			},
		},
		{
			name: "invalid fixed segment",
			source: `{
				"banks": {
					"main:40": {}
				}
			}`,
			expectedError: "invalid address segment: main:40",
		},
		{
			name: "invalid subsegment type",
			source: `{
				"banks": {
					"main": 42
				}
			}`,
			expectedError: "invalid segment",
		},
		{
			name: "pattern on fixed segment",
			source: `{
				"banks": {
					"main": {
						".pattern": "^[0-9]{3}$"
					}
				}
			}`,
			expectedError: "cannot have a pattern on a fixed segment",
		},
		{
			name: "two variable segments with same prefix",
			source: `{
				"users": {
					"$userID": {
						".pattern": "^[0-9]{3}$"
					},
					"$otherID": {
						".pattern": "^[0-9]{4}$"
					}
				}
			}`,
			expectedError: "cannot have two variable segments with the same prefix",
		},
		{
			name: "variable segment without a pattern",
			source: `{
				"users": {
					"$userID": {
						".metadata": {
							"key": "value"
						}
					}
				}
			}`,
			expectedError: "cannot have a variable segment without a pattern",
		},
		{
			name: "invalid metadata",
			source: `{
				"banks": {
					"main": {
						".metadata": 42
					}
				}
			}`,
			expectedError: "invalid default metadata",
		},
		{
			name: "invalid rules",
			source: `{
				"banks": {
					"main": {
						".rules": 42
					}
				}
			}`,
			expectedError: "invalid account rules",
		},
		{
			name: "invalid account schema",
			source: `{
				"banks": {
					"main": {
						".self": {
							".rules": {}
						}
					}
				}
			}`,
			expectedError: "invalid segment",
		},
		{
			name:          "root account",
			source:        `{ ".self": { } }`,
			expectedError: "root cannot be an account",
		},
		{
			name:          "variable segment at root",
			source:        `{ "$banks": { ".pattern": "^[0-9]+$", ".self": {} } }`,
			expectedError: "root cannot have a variable segment",
		},
		{
			name:          "invalid root subsegment name",
			source:        `{ "abc:abc": { ".self": {} } }`,
			expectedError: "invalid segment name",
		},
		{
			name:          "non-string pattern",
			source:        `{ "banks": { "$bankID": { ".pattern": 42 } } }`,
			expectedError: "pattern must be a string",
		},
		{
			name:          "invalid pattern regex",
			source:        `{ "banks": { "$bankID": { ".pattern": "[[" } } }`,
			expectedError: "invalid pattern regex",
		},
		{
			name: "non-object self",
			source: `{ "foo": {
					".self": 42,
					"bar": { "baz": {} }
			} }`,
			expectedError: ".self must be an empty object",
		},
		{
			name: "self with extra fields",
			source: `{ "foo": {
				".self": {
					"key": "value"
				},
				"bar": { "baz": {} }
			} }`,
			expectedError: ".self must be an empty object",
		},
	} {
		var chart ChartOfAccounts
		err := json.Unmarshal([]byte(tc.source), &chart)

		if tc.expectedError == "" {
			require.NoError(t, err, tc.name)

			require.Equal(t, tc.expectedChart, chart, tc.name)

			value, err := json.MarshalIndent(&chart, "", "    ")
			require.NoError(t, err, tc.name)
			require.JSONEq(t, tc.source, string(value), tc.name)
		} else {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		}
	}
}

func testChart() ChartOfAccounts {
	return ChartOfAccounts{
		"bank": {
			VariableSegment: &ChartVariableSegment{
				Label:   "bankID",
				Pattern: "^[0-9]{3}$",
				ChartSegment: ChartSegment{
					Account: &ChartAccount{
						Rules: ChartAccountRules{},
						Metadata: map[string]string{
							"account_is": "bank subaccount",
						},
					},
				},
			},
			Account: &ChartAccount{
				Metadata: map[string]string{
					"account_is": "main bank account",
				},
			},
		},
		"users": {
			VariableSegment: &ChartVariableSegment{
				Label:   "userID",
				Pattern: "^[0-9]{3}$",
				ChartSegment: ChartSegment{
					FixedSegments: map[string]ChartSegment{
						"main": {
							Account: &ChartAccount{
								Metadata: map[string]string{
									"account_is": "main user account",
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestAccountValidation(t *testing.T) {
	t.Parallel()

	chart := testChart()

	type testCase struct {
		name            string
		address         string
		expectedAccount *ChartAccount
		expectedError   string
	}

	for _, tc := range []testCase{
		{
			name:            "always find world",
			address:         "world",
			expectedAccount: &ChartAccount{},
		},
		{
			name:    "non-leaf account",
			address: "bank",
			expectedAccount: &ChartAccount{
				Metadata: map[string]string{
					"account_is": "main bank account",
				},
			},
		},
		{
			name:    "leaf account",
			address: "bank:012",
			expectedAccount: &ChartAccount{
				Metadata: map[string]string{
					"account_is": "bank subaccount",
				},
			},
		},
		{
			name:    "address with inner variable segment",
			address: "users:001:main",
			expectedAccount: &ChartAccount{
				Metadata: map[string]string{
					"account_is": "main user account",
				},
			},
		},
		{
			name:          "invalid variable segment",
			address:       "users:abc:main",
			expectedError: "segment `abc` is not allowed by the chart of accounts at `[users]`",
		},
		{
			name:          "non-account variable branch",
			address:       "users:001",
			expectedError: "segment `001` is not allowed by the chart of accounts at `[users]`",
		},
		{
			name:          "non-account fixed branch",
			address:       "users",
			expectedError: "segment `users` is not allowed by the chart of accounts at `[]`",
		},
	} {
		if tc.expectedAccount != nil {
			acc, err := chart.FindAccountSchema(tc.address)
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedAccount, acc, tc.name)
		} else {
			_, err := chart.FindAccountSchema(tc.address)
			require.EqualError(t, err, tc.expectedError, tc.name)
		}
	}
}

func TestPostingValidation(t *testing.T) {
	t.Parallel()

	chart := testChart()

	type testCase struct {
		name        string
		posting     Posting
		expectError bool
	}

	for _, tc := range []testCase{
		{
			name: "valid posting",
			posting: Posting{
				Source:      "bank:012",
				Destination: "users:012:main",
			},
		},
		{
			name: "",
			posting: Posting{
				Source:      "bank:invalid",
				Destination: "users:001:main",
			},
			expectError: true,
		},
		{
			name: "",
			posting: Posting{
				Source:      "bank:012",
				Destination: "users:invalid:main",
			},
			expectError: true,
		},
	} {
		if tc.expectError {
			err := chart.ValidatePosting(tc.posting)
			require.ErrorIs(t, err, ErrInvalidAccount{}, tc.name)
		} else {
			err := chart.ValidatePosting(tc.posting)
			require.NoError(t, err, tc.name)
		}
	}
}
