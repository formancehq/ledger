package ledger_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoScript(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{}

		_, err := l.Execute(context.Background(), nil, script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorNoScript, err.(*ledger.ScriptError).Code)
	})
}

func TestCompilationError(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{
			ScriptCore: core.ScriptCore{Plain: "willnotcompile"},
		}

		_, err := l.Execute(context.Background(), nil, script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorCompilationFailed, err.(*ledger.ScriptError).Code)
	})
}

func TestSend(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: `
					send [USD/2 99] (
						source=@world
						destination=@user:001
					)`,
			},
		}

		_, err := l.Execute(context.Background(), nil, script)
		require.NoError(t, err)

		assertBalance(t, l, "user:001",
			"USD/2", core.NewMonetaryInt(99))
	})
}

func TestNoVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: `
					vars {
						account $dest
					}

					send [CAD/2 42] (
						source = @world
						destination = $dest
					)`,
				Vars: map[string]json.RawMessage{},
			},
		}

		_, err := l.Execute(context.Background(), nil, script)
		assert.Error(t, err)

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: `
					vars {
						account $dest
					}

					send [CAD/2 42] (
						source = @world
						destination = $dest
					)`,
				Vars: map[string]json.RawMessage{
					"dest": json.RawMessage(`"user:042"`),
				},
			},
		}

		_, err := l.Execute(context.Background(), nil, script)
		require.NoError(t, err)

		assertBalance(t, l, "user:042",
			"CAD/2", core.NewMonetaryInt(42))
	})
}

func TestEnoughFunds(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:001",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}

		_, err := l.Commit(context.Background(), nil, tx)
		require.NoError(t, err)

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: `
					send [COIN 95] (
						source = @user:001
						destination = @world
					)`,
			},
		}

		_, err = l.Execute(context.Background(), nil, script)
		assert.NoError(t, err)
	})
}

func TestNotEnoughFunds(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:002",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}

		_, err := l.Commit(context.Background(), nil, tx)
		require.NoError(t, err)

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: `
					send [COIN 105] (
						source = @user:002
						destination = @world
					)`,
			},
		}

		_, err = l.Execute(context.Background(), nil, script)
		assert.True(t, ledger.IsScriptErrorWithCode(err, apierrors.ErrInsufficientFund))
	})
}

func TestMissingMetadata(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		plain := `
			vars {
				account $sale
				account $seller = meta($sale, "seller")
			}

			send [COIN *] (
				source = $sale
				destination = $seller
			)`
		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: plain,
				Vars: map[string]json.RawMessage{
					"sale": json.RawMessage(`"sales:042"`),
				},
			},
		}

		_, err := l.Execute(context.Background(), nil, script)
		assert.True(t, ledger.IsScriptErrorWithCode(err, ledger.ScriptErrorCompilationFailed))
	})
}

func TestMetadata(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "sales:042",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}

		_, err := l.Commit(context.Background(), nil, tx)
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount,
			"sales:042",
			core.Metadata{
				"seller": json.RawMessage(`{
					"type":  "account",
					"value": "users:053"
				}`),
			})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount,
			"users:053",
			core.Metadata{
				"commission": json.RawMessage(`{
					"type":  "portion",
					"value": "15.5%"
				}`),
			})
		require.NoError(t, err)

		plain := `
			vars {
				account $sale
				account $seller = meta($sale, "seller")
				portion $commission = meta($seller, "commission")
			}

			send [COIN *] (
				source = $sale
				destination = {
					remaining to $seller
					$commission to @platform
				}
			)
		`
		require.NoError(t, err)

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: plain,
				Vars: map[string]json.RawMessage{
					"sale": json.RawMessage(`"sales:042"`),
				},
			},
		}

		_, err = l.Execute(context.Background(), nil, script)
		require.NoError(t, err)

		assertBalance(t, l, "sales:042", "COIN", core.NewMonetaryInt(0))

		assertBalance(t, l, "users:053", "COIN", core.NewMonetaryInt(85))

		assertBalance(t, l, "platform", "COIN", core.NewMonetaryInt(15))
	})
}

func TestSetTxMeta(t *testing.T) {
	type testCase struct {
		name              string
		script            core.Script
		expectedMetadata  core.Metadata
		expectedErrorCode string
	}
	for _, tc := range []testCase{
		{
			name: "nominal",
			script: core.Script{
				ScriptCore: core.ScriptCore{
					Plain: `
					send [USD/2 99] (
						source=@world
						destination=@user:001
					)`,
				},
				Metadata: core.Metadata{
					"priority": "low",
				},
			},
			expectedMetadata: core.Metadata{
				"priority": "low",
			},
		},
		{
			name: "define metadata on script",
			script: core.Script{
				ScriptCore: core.ScriptCore{
					Plain: `
					set_tx_meta("priority", "low")

					send [COIN 10] (
						source = @world
						destination = @user:001
					)`,
				},
			},
			expectedMetadata: core.Metadata{
				"priority": map[string]any{"type": "string", "value": "low"},
			},
		},
		{
			name: "override metadata of script",
			script: core.Script{
				ScriptCore: core.ScriptCore{
					Plain: `
					set_tx_meta("priority", "low")

					send [USD/2 99] (
						source=@world
						destination=@user:001
					)`,
				},
				Metadata: core.Metadata{
					"priority": "high",
				},
			},
			expectedErrorCode: ledger.ScriptErrorMetadataOverride,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runOnLedger(func(l *ledger.Ledger) {
				defer func(l *ledger.Ledger, ctx context.Context) {
					require.NoError(t, l.Close(ctx))
				}(l, context.Background())

				_, err := l.Execute(context.Background(), nil, tc.script)

				if tc.expectedErrorCode != "" {
					require.Error(t, err)
					require.True(t, ledger.IsScriptErrorWithCode(err, tc.expectedErrorCode))
				} else {
					require.NoError(t, err)
					last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
					require.NoError(t, err)
					assert.True(t, last.Metadata.IsEquivalentTo(tc.expectedMetadata))
				}
			})
		})
	}
}

func TestScriptSetReference(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		plain := `
			send [USD/2 99] (
				source=@world
				destination=@user:001
			)`

		script := core.Script{
			ScriptCore: core.ScriptCore{
				Plain: plain,
				Vars:  map[string]json.RawMessage{},
			},
			Reference: "tx_ref",
		}

		_, err := l.Execute(context.Background(), nil, script)
		require.NoError(t, err)

		last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		assert.Equal(t, script.Reference, last.Reference)
	})
}

func TestSetAccountMeta(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		t.Run("valid", func(t *testing.T) {
			res, _, err := l.ProcessScript(context.Background(), nil, core.Script{
				ScriptCore: core.ScriptCore{Plain: `
					set_account_meta(@alice, "aaa", "string meta")
					set_account_meta(@alice, "bbb", 42)
					set_account_meta(@alice, "ccc", COIN)
					set_account_meta(@alice, "ddd", [COIN 30])
					set_account_meta(@alice, "eee", @bob)
					`,
				},
			})
			require.NoError(t, err)
			require.Equal(t, core.Metadata{
				"set_account_meta": map[string]any{
					"alice": map[string]any{
						"aaa": map[string]any{"type": "string", "value": "string meta"},
						"bbb": map[string]any{"type": "number", "value": 42.},
						"ccc": map[string]any{"type": "asset", "value": "COIN"},
						"ddd": map[string]any{"type": "monetary",
							"value": map[string]any{"asset": "COIN", "amount": 30.}},
						"eee": map[string]any{"type": "account", "value": "bob"},
					},
				},
			}, res.Metadata)
		})

		t.Run("invalid syntax", func(t *testing.T) {
			_, _, err := l.ProcessScript(context.Background(), nil, core.Script{
				ScriptCore: core.ScriptCore{Plain: `
					set_account_meta(@bob, "is")`,
				},
			})
			require.True(t, ledger.IsScriptErrorWithCode(err,
				ledger.ScriptErrorCompilationFailed))
		})
	})
}

func assertBalance(t *testing.T, l *ledger.Ledger, account, asset string, amount *core.MonetaryInt) {
	user, err := l.GetAccount(context.Background(), account)
	require.NoError(t, err)

	b := user.Balances[asset]
	assert.Equalf(t, amount.String(), b.String(),
		"wrong %v balance for account %v, expected: %s got: %s",
		asset, account,
		amount, b,
	)
}
