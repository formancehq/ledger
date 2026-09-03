package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
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
            "main": {},
            "out": {
                ".metadata": {
                    "foo": {},
                    "bar": {
                        "default": "BAR"
                    }
                }
            },
            "pending_out": {}
        }
    },
    "users": {
        "$userID": {
            ".self": {},
            "main": {}
        }
    }
}`,
			expectedChart: ChartOfAccounts{
				"banks": {
					VariableSegments: []ChartVariableSegment{{
						Label:   "iban",
						Pattern: pointer.For("^[0-9]{10}$"),
						ChartSegment: ChartSegment{
							FixedSegments: map[string]ChartSegment{
								"main": {
									Account: &ChartAccount{
										Rules: ChartAccountRules{},
									},
								},
								"out": {
									Account: &ChartAccount{
										Metadata: map[string]ChartAccountMetadata{
											"foo": {},
											"bar": {
												Default: pointer.For("BAR"),
											},
										},
									},
								},
								"pending_out": {
									Account: &ChartAccount{},
								},
							},
						},
					}},
				},
				"users": {
					VariableSegments: []ChartVariableSegment{{
						Label:   "userID",
						Pattern: nil,
						ChartSegment: ChartSegment{
							Account: &ChartAccount{},
							FixedSegments: map[string]ChartSegment{
								"main": {
									Account: &ChartAccount{},
								},
							},
						},
					}},
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
			name: "pattern on fixed root segment",
			source: `{
				"banks": {
					".pattern": "^[0-9]{3}$"
				}
			}`,
			expectedError: "cannot have a pattern on a fixed segment",
		},
		{
			name: "metadata on non-account segment",
			source: `{
				"banks": {
					".metadata": {},
					"main": {}
				}
			}`,
			expectedError: "cannot have .metadata on a non-account segment",
		},
		{
			name: "rules on non-account segment",
			source: `{
				"banks": {
					".rules": {},
					"main": {}
				}
			}`,
			expectedError: "cannot have .rules on a non-account segment",
		},
		{
			name: "several variable segments at the same level",
			source: `{
    "users": {
        "$userID": {
            ".pattern": "^[0-9]{3}$",
            "main": {}
        },
        "$username": {
            ".pattern": "^[a-z]+$"
        }
    }
}`,
			expectedChart: ChartOfAccounts{
				"users": {
					VariableSegments: []ChartVariableSegment{
						{
							Label:   "userID",
							Pattern: pointer.For("^[0-9]{3}$"),
							ChartSegment: ChartSegment{
								FixedSegments: map[string]ChartSegment{
									"main": {
										Account: &ChartAccount{},
									},
								},
							},
						},
						{
							Label:   "username",
							Pattern: pointer.For("^[a-z]+$"),
							ChartSegment: ChartSegment{
								Account: &ChartAccount{},
							},
						},
					},
				},
			},
		},
		{
			name: "several variable segments with a missing pattern",
			source: `{
				"users": {
					"$userID": {
						".pattern": "^[0-9]{3}$"
					},
					"$username": {}
				}
			}`,
			expectedError: "variable segment $username needs a .pattern",
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

			require.Equal(t, tc.expectedChart, chart, "data structure: %s", tc.name)

			value, err := json.MarshalIndent(&chart, "", "    ")
			require.NoError(t, err, tc.name)
			require.JSONEq(t, tc.source, string(value), "roundtrip: %s", tc.name)
		} else {
			require.ErrorContains(t, err, tc.expectedError, tc.name)
		}
	}
}

func testChart() ChartOfAccounts {
	return ChartOfAccounts{
		"world": {
			Account: &ChartAccount{},
		},
		"bank": {
			VariableSegments: []ChartVariableSegment{{
				Label:   "bankID",
				Pattern: pointer.For("^[0-9]{3}$"),
				ChartSegment: ChartSegment{
					Account: &ChartAccount{
						Rules: ChartAccountRules{},
						Metadata: map[string]ChartAccountMetadata{
							"bank_subaccount": {
								Default: pointer.For("test"),
							},
						},
					},
				},
			}},
			Account: &ChartAccount{
				Metadata: map[string]ChartAccountMetadata{
					"root_bank_account": {},
				},
			},
		},
		"users": {
			VariableSegments: []ChartVariableSegment{{
				Label:   "userID",
				Pattern: pointer.For("^[0-9]{3}$"),
				ChartSegment: ChartSegment{
					FixedSegments: map[string]ChartSegment{
						"main": {
							Account: &ChartAccount{
								Metadata: map[string]ChartAccountMetadata{},
							},
						},
					},
				},
			}},
		},
		"shops": {
			VariableSegments: []ChartVariableSegment{{
				Label: "shopID",
				ChartSegment: ChartSegment{
					Account: &ChartAccount{
						Metadata: map[string]ChartAccountMetadata{
							"shop_account": {
								Default: pointer.For("foo"),
							},
						},
					},
				},
			}},
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
				Metadata: map[string]ChartAccountMetadata{
					"root_bank_account": {},
				},
			},
		},
		{
			name:    "leaf account",
			address: "bank:012",
			expectedAccount: &ChartAccount{
				Metadata: map[string]ChartAccountMetadata{
					"bank_subaccount": {
						Default: pointer.For("test"),
					},
				},
			},
		},
		{
			name:    "address with inner variable segment",
			address: "users:001:main",
			expectedAccount: &ChartAccount{
				Metadata: map[string]ChartAccountMetadata{},
			},
		},
		{
			name:    "patternless variable segment",
			address: "shops:whatever_should-WORK0123",
			expectedAccount: &ChartAccount{
				Metadata: map[string]ChartAccountMetadata{
					"shop_account": {
						Default: pointer.For("foo"),
					},
				},
			},
		},
		{
			name:          "invalid root segment",
			address:       "bonk:012",
			expectedError: "account starting with `bonk` is not defined in the chart of accounts",
		},
		{
			name:          "invalid variable segment",
			address:       "bank:invalid",
			expectedError: "segment `invalid` defined by the chart of accounts at `bank` does not match the pattern",
		},
		{
			name:          "invalid non-root non-variable segment",
			address:       "users:001:nope",
			expectedError: "segment `nope` is not allowed by the chart of accounts at `users:001`",
		},
		{
			name:          "non-account variable branch",
			address:       "users:001",
			expectedError: "segment `001` is not allowed by the chart of accounts at `users`",
		},
		{
			name:          "non-account fixed branch",
			address:       "users",
			expectedError: "account `users` is not defined in the chart of accounts",
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
			name: "invalid source",
			posting: Posting{
				Source:      "bank:invalid",
				Destination: "users:001:main",
			},
			expectError: true,
		},
		{
			name: "invalid destination",
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

// Barrier accounts carry a `_TYPE` suffix and must never hold a client
// subaccount, while a bare stub may. The two shapes are kept apart by giving
// the account level one variable segment per shape, each gated by its own
// pattern -- a single `$accountId` whose pattern makes the suffix optional
// cannot express it, since `.pattern` only sees the segment in isolation and
// has no visibility on whether a `client` child was addressed below it.
const barrierChart = `{
	"world": {},
	"bank": {
		"$bankId": {
			".pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
			"branch": {
				"$branchId": {
					".pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
					"account": {
						"$accountId": {
							".self": {},
							".pattern": "^(?<stub>[A-Z]{4})$",
							"client": {
								"$clientId": {
									".pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
								}
							}
						},
						"$barrierAccountId": {
							".pattern": "^(?<stub>[A-Z]{4})_(?<type>TRADING|TOPUP|UNIDENTIFIED|CLEARED)$"
						}
					}
				}
			}
		}
	}
}`

func TestBarrierAccountValidation(t *testing.T) {
	t.Parallel()

	var chart ChartOfAccounts
	require.NoError(t, json.Unmarshal([]byte(barrierChart), &chart))

	const (
		bankID   = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
		branchID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		clientID = "c73bcdcc-2669-4bf6-81d3-e4ae73fb11fd"
	)
	accounts := "bank:" + bankID + ":branch:" + branchID + ":account:"

	type testCase struct {
		name          string
		address       string
		expectedError string
	}

	for _, tc := range []testCase{
		{
			name:    "plain account with a client",
			address: accounts + "LSER:client:" + clientID,
		},
		{
			name:    "plain account without a client",
			address: accounts + "LSER",
		},
		{
			name:    "barrier account without a client",
			address: accounts + "LSER_TRADING",
		},
		{
			name:    "every barrier type is addressable",
			address: accounts + "LSER_UNIDENTIFIED",
		},
		{
			name:          "barrier account with a client",
			address:       accounts + "LSER_TRADING:client:" + clientID,
			expectedError: "segment `client` is not allowed by the chart of accounts at `" + accounts + "LSER_TRADING`",
		},
		{
			name:          "barrier account with an empty client subtree",
			address:       accounts + "LSER_CLEARED:client",
			expectedError: "segment `client` is not allowed by the chart of accounts at `" + accounts + "LSER_CLEARED`",
		},
		{
			name:          "unknown barrier type",
			address:       accounts + "LSER_SETTLED",
			expectedError: "segment `LSER_SETTLED` defined by the chart of accounts at `bank:" + bankID + ":branch:" + branchID + ":account` does not match the pattern",
		},
		{
			name:          "client of an unknown barrier type",
			address:       accounts + "LSER_SETTLED:client:" + clientID,
			expectedError: "segment `LSER_SETTLED` defined by the chart of accounts at `bank:" + bankID + ":branch:" + branchID + ":account` does not match the pattern",
		},
		{
			name:          "plain account with an invalid client",
			address:       accounts + "LSER:client:not-a-uuid",
			expectedError: "segment `not-a-uuid` defined by the chart of accounts at `" + accounts + "LSER:client` does not match the pattern",
		},
	} {
		_, err := chart.FindAccountSchema(tc.address)
		if tc.expectedError == "" {
			require.NoError(t, err, tc.name)
		} else {
			require.EqualError(t, err, tc.expectedError, tc.name)
		}
	}
}

// Postings are the enforcement point: a barrier account with a client
// underneath it must be rejected whichever side of the posting it sits on.
func TestBarrierPostingValidation(t *testing.T) {
	t.Parallel()

	var chart ChartOfAccounts
	require.NoError(t, json.Unmarshal([]byte(barrierChart), &chart))

	const (
		bankID   = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
		branchID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		clientID = "c73bcdcc-2669-4bf6-81d3-e4ae73fb11fd"
	)
	accounts := "bank:" + bankID + ":branch:" + branchID + ":account:"

	type testCase struct {
		name        string
		posting     Posting
		expectError bool
	}

	for _, tc := range []testCase{
		{
			name: "client account funded from a barrier account",
			posting: Posting{
				Source:      accounts + "LSER_TOPUP",
				Destination: accounts + "LSER:client:" + clientID,
			},
		},
		{
			name: "barrier account as destination of a client account",
			posting: Posting{
				Source:      accounts + "LSER:client:" + clientID,
				Destination: accounts + "LSER_TRADING",
			},
		},
		{
			name: "client of a barrier account as source",
			posting: Posting{
				Source:      accounts + "LSER_TRADING:client:" + clientID,
				Destination: "world",
			},
			expectError: true,
		},
		{
			name: "client of a barrier account as destination",
			posting: Posting{
				Source:      "world",
				Destination: accounts + "LSER_TRADING:client:" + clientID,
			},
			expectError: true,
		},
	} {
		err := chart.ValidatePosting(tc.posting)
		if tc.expectError {
			require.ErrorIs(t, err, ErrInvalidAccount{}, tc.name)
		} else {
			require.NoError(t, err, tc.name)
		}
	}
}
