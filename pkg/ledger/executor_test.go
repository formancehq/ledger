package ledger_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoScript(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.ScriptData{}

		_, err := l.Execute(context.Background(), false, false, script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorNoScript, err.(*ledger.ScriptError).Code)
	})
}

func TestCompilationError(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.ScriptData{
			Script: core.Script{Plain: "willnotcompile"},
		}

		_, err := l.Execute(context.Background(), false, false, script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorCompilationFailed, err.(*ledger.ScriptError).Code)
	})
}

func TestSend(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
					send [USD/2 99] (
						source=@world
						destination=@user:001
					)`,
			},
		}

		_, err := l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		assertBalance(t, l, "user:001",
			"USD/2", core.NewMonetaryInt(99))
	})
}

func TestNoVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.ScriptData{
			Script: core.Script{
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

		_, err := l.Execute(context.Background(), false, false, script)
		assert.Error(t, err)

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.ScriptData{
			Script: core.Script{
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

		_, err := l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		assertBalance(t, l, "user:042",
			"CAD/2", core.NewMonetaryInt(42))
	})
}

func TestVariablesEmptyAccount(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
					send [EUR 1] (
						source = @world
						destination = @bob
					)`,
			},
		}
		_, err := l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		script = core.ScriptData{
			Script: core.Script{
				Plain: `
					vars {
						account $acc
					}

					send [EUR 1] (
						source = {
							@bob
							$acc
						}
						destination = @alice
					)`,
				Vars: map[string]json.RawMessage{
					"acc": json.RawMessage(`""`),
				},
			},
		}
		_, err = l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		assertBalance(t, l, "alice", "EUR", core.NewMonetaryInt(1))
		assertBalance(t, l, "bob", "EUR", core.NewMonetaryInt(0))
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

		_, err := l.Execute(context.Background(),
			true, false, core.TxsToScriptsData(tx)...)
		require.NoError(t, err)

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
 					send [COIN 95] (
 						source = @user:001
 						destination = @world
 					)`,
			},
		}

		_, err = l.Execute(context.Background(), false, false, script)
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

		_, err := l.Execute(context.Background(),
			true, false, core.TxsToScriptsData(tx)...)
		require.NoError(t, err)

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
 					send [COIN 105] (
 						source = @user:002
 						destination = @world
 					)`,
			},
		}

		_, err = l.Execute(context.Background(), false, false, script)
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
		script := core.ScriptData{
			Script: core.Script{
				Plain: plain,
				Vars: map[string]json.RawMessage{
					"sale": json.RawMessage(`"sales:042"`),
				},
			},
		}

		_, err := l.Execute(context.Background(), false, false, script)
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

		_, err := l.Execute(context.Background(),
			true, false, core.TxsToScriptsData(tx)...)
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

		script := core.ScriptData{
			Script: core.Script{
				Plain: plain,
				Vars: map[string]json.RawMessage{
					"sale": json.RawMessage(`"sales:042"`),
				},
			},
		}

		_, err = l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		assertBalance(t, l, "sales:042", "COIN", core.NewMonetaryInt(0))
		assertBalance(t, l, "users:053", "COIN", core.NewMonetaryInt(85))
		assertBalance(t, l, "platform", "COIN", core.NewMonetaryInt(15))
	})
}

func TestSetTxMeta(t *testing.T) {
	type testCase struct {
		name              string
		script            core.ScriptData
		expectedMetadata  core.Metadata
		expectedErrorCode string
	}
	for _, tc := range []testCase{
		{
			name: "nominal",
			script: core.ScriptData{
				Script: core.Script{
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
			script: core.ScriptData{
				Script: core.Script{
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
			script: core.ScriptData{
				Script: core.Script{
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

				_, err := l.Execute(context.Background(), false, false, tc.script)

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

		script := core.ScriptData{
			Script: core.Script{
				Plain: plain,
				Vars:  map[string]json.RawMessage{},
			},
			Reference: "tx_ref",
		}

		_, err := l.Execute(context.Background(), false, false, script)
		require.NoError(t, err)

		last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		assert.Equal(t, script.Reference, last.Reference)
	})
}

func TestScriptReferenceConflict(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		_, err := l.Execute(context.Background(), false, false,
			core.ScriptData{
				Script: core.Script{
					Plain: `
				send [USD/2 99] (
					source=@world
					destination=@user:001
				)`,
					Vars: map[string]json.RawMessage{},
				},
				Reference: "tx_ref",
			})
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), false, false,
			core.ScriptData{
				Script: core.Script{
					Plain: `
				send [USD/2 99] (
					source=@unexists
					destination=@user:001
				)`,
					Vars: map[string]json.RawMessage{},
				},
				Reference: "tx_ref",
			})
		require.Error(t, err)
		require.True(t, ledger.IsConflictError(err))
	})
}

func TestSetAccountMeta(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		t.Run("valid", func(t *testing.T) {
			res, err := l.Execute(context.Background(),
				false, false, core.ScriptData{
					Script: core.Script{Plain: `
						send [USD/2 99] (
							source = @world
							destination = @user:001
						)
						set_account_meta(@alice, "aaa", "string meta")
						set_account_meta(@alice, "bbb", 42)
						set_account_meta(@alice, "ccc", COIN)
						set_account_meta(@alice, "ddd", [COIN 30])
						set_account_meta(@alice, "eee", @bob)
  					`},
				})
			require.NoError(t, err)
			require.Equal(t, 1, len(res))

			acc, err := l.GetAccount(context.Background(), "alice")
			require.NoError(t, err)
			require.Equal(t, core.Metadata{
				"aaa": map[string]any{"type": "string", "value": "string meta"},
				"bbb": map[string]any{"type": "number", "value": 42.},
				"ccc": map[string]any{"type": "asset", "value": "COIN"},
				"ddd": map[string]any{"type": "monetary",
					"value": map[string]any{"asset": "COIN", "amount": 30.}},
				"eee": map[string]any{"type": "account", "value": "bob"},
			}, acc.Metadata)
		})

		t.Run("invalid syntax", func(t *testing.T) {
			_, err := l.Execute(context.Background(), false, false,
				core.ScriptData{
					Script: core.Script{Plain: `
						send [USD/2 99] (
							source = @world
							destination = @user:001
						)
						set_account_meta(@bob, "is")
					`},
				})
			require.True(t, ledger.IsScriptErrorWithCode(err,
				ledger.ScriptErrorCompilationFailed))
		})
	})
}

func TestMonetaryVariableBalance(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
			defer func(l *ledger.Ledger, ctx context.Context) {
				require.NoError(t, l.Close(ctx))
			}(l, context.Background())

			tx := core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "users:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
			}
			_, err := l.Execute(context.Background(),
				true, false, core.TxsToScriptsData(tx)...)
			require.NoError(t, err)

			script := core.ScriptData{
				Script: core.Script{
					Plain: `
 					vars {
 						monetary $bal = balance(@users:001, COIN)
 					}
 					send $bal (
 						source = @users:001
 						destination = @world
 					)`,
				},
			}

			_, err = l.Execute(context.Background(),
				false, false, script)
			require.NoError(t, err)
			assertBalance(t, l, "world", "COIN", core.NewMonetaryInt(0))
			assertBalance(t, l, "users:001", "COIN", core.NewMonetaryInt(0))
		})
	})

	t.Run("complex", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
			defer func(l *ledger.Ledger, ctx context.Context) {
				require.NoError(t, l.Close(ctx))
			}(l, context.Background())

			tx := core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "A",
						Amount:      core.NewMonetaryInt(40),
						Asset:       "USD/2",
					},
					{
						Source:      "world",
						Destination: "C",
						Amount:      core.NewMonetaryInt(90),
						Asset:       "USD/2",
					},
				},
			}
			_, err := l.Execute(context.Background(),
				true, false, core.TxsToScriptsData(tx)...)
			require.NoError(t, err)

			script := core.ScriptData{
				Script: core.Script{
					Plain: `
 				vars {
 				  monetary $initial = balance(@A, USD/2)
 				}
 				send [USD/2 100] (
 				  source = {
 					@A
 					@C
 				  }
 				  destination = {
 					max $initial to @B
 					remaining to @D
 				  }
 				)`,
				},
			}

			_, err = l.Execute(context.Background(),
				false, false, script)
			require.NoError(t, err)
			assertBalance(t, l, "B", "USD/2", core.NewMonetaryInt(40))
			assertBalance(t, l, "D", "USD/2", core.NewMonetaryInt(60))
		})
	})

	t.Run("error insufficient funds", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
			defer func(l *ledger.Ledger, ctx context.Context) {
				require.NoError(t, l.Close(ctx))
			}(l, context.Background())

			tx := core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "users:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
			}
			_, err := l.Execute(context.Background(),
				true, false, core.TxsToScriptsData(tx)...)
			require.NoError(t, err)

			script := core.ScriptData{
				Script: core.Script{
					Plain: `
 					vars {
 						monetary $bal = balance(@users:001, COIN)
 					}
 					send $bal (
 						source = @users:001
 						destination = @world
 					)
 					send $bal (
 						source = @users:001
 						destination = @world
 					)`,
				},
			}
			_, err = l.Execute(context.Background(),
				false, false, script)
			assert.True(t, ledger.IsScriptErrorWithCode(err, apierrors.ErrInsufficientFund))
		})
	})

	t.Run("error negative balance", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
			defer func(l *ledger.Ledger, ctx context.Context) {
				require.NoError(t, l.Close(ctx))
			}(l, context.Background())

			tx := core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "users:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
			}
			_, err := l.Execute(context.Background(),
				true, false, core.TxsToScriptsData(tx)...)
			require.NoError(t, err)

			script := core.ScriptData{
				Script: core.Script{
					Plain: `
 					vars {
 						monetary $bal = balance(@world, COIN)
 					}
 					send $bal (
 						source = @users:001
 						destination = @world
 					)`,
				},
			}

			_, err = l.Execute(context.Background(), false, false, script)
			assert.True(t, ledger.IsScriptErrorWithCode(err, ledger.ScriptErrorCompilationFailed))
			assert.ErrorContains(t, err, "must be non-negative")
		})
	})

	t.Run("error variable type", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
			defer func(l *ledger.Ledger, ctx context.Context) {
				require.NoError(t, l.Close(ctx))
			}(l, context.Background())

			script := core.ScriptData{
				Script: core.Script{
					Plain: `
 					vars {
 						account $bal = balance(@users:001, COIN)
 					}
 					send $bal (
 						source = @users:001
 						destination = @world
 					)`,
				},
			}
			_, err := l.Execute(context.Background(), false, false, script)
			assert.True(t, ledger.IsScriptErrorWithCode(err, apierrors.ErrScriptCompilationFailed))
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

var execRes []core.ExpandedTransaction

const nbPostings = 1000

func BenchmarkLedger_PostTransactionsSingle(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		txData := core.TransactionData{}
		for i := 0; i < nbPostings; i++ {
			txData.Postings = append(txData.Postings, core.Posting{
				Source:      "world",
				Destination: "benchmarks:" + strconv.Itoa(i),
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(10),
			})
		}

		b.ResetTimer()

		res := []core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			_, err := txData.Postings.Validate()
			require.NoError(b, err)
			script := core.TxsToScriptsData(txData)
			res, err = l.Execute(context.Background(), true, true, script...)
			require.NoError(b, err)
			require.Len(b, res, 1)
			require.Len(b, res[0].Postings, nbPostings)
		}

		execRes = res
		require.Len(b, execRes, 1)
		require.Len(b, execRes[0].Postings, nbPostings)
	})
}

func newTxsData(i int) []core.TransactionData {
	return []core.TransactionData{
		{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: fmt.Sprintf("payins:%d", i),
					Amount:      core.NewMonetaryInt(10000),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("payins:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(10000),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: fmt.Sprintf("teller:%d", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
				{
					Source:      "world",
					Destination: fmt.Sprintf("teller:%d", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(1500),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("fiat:holdings:%d", i),
					Amount:      core.NewMonetaryInt(1500),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("teller:%d", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(4230),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("fiat:holdings:%d", i),
					Amount:      core.NewMonetaryInt(4230),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("teller:%d", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("users:%d:withdrawals", i),
					Amount:      core.NewMonetaryInt(2270),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:withdrawals", i),
					Destination: fmt.Sprintf("payouts:%d", i),
					Amount:      core.NewMonetaryInt(2270),
					Asset:       "EUR/2",
				},
			},
		},
	}
}

func BenchmarkLedger_PostTransactionsBatch(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		txsData := newTxsData(1)

		b.ResetTimer()

		res := []core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			var err error
			for _, txData := range txsData {
				_, err := txData.Postings.Validate()
				require.NoError(b, err)
			}
			script := core.TxsToScriptsData(txsData...)
			res, err = l.Execute(context.Background(), true, true, script...)
			require.NoError(b, err)
			require.Len(b, res, 7)
			require.Len(b, res[0].Postings, 1)
			require.Len(b, res[1].Postings, 1)
			require.Len(b, res[2].Postings, 2)
			require.Len(b, res[3].Postings, 4)
			require.Len(b, res[4].Postings, 4)
			require.Len(b, res[5].Postings, 1)
			require.Len(b, res[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes, 7)
		require.Len(b, execRes[0].Postings, 1)
		require.Len(b, execRes[1].Postings, 1)
		require.Len(b, execRes[2].Postings, 2)
		require.Len(b, execRes[3].Postings, 4)
		require.Len(b, execRes[4].Postings, 4)
		require.Len(b, execRes[5].Postings, 1)
		require.Len(b, execRes[6].Postings, 1)
	})
}

func BenchmarkLedger_PostTransactionsBatch2(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		b.ResetTimer()

		res := []core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			b.StopTimer()
			txsData := newTxsData(n)
			b.StartTimer()
			var err error
			for _, txData := range txsData {
				_, err := txData.Postings.Validate()
				require.NoError(b, err)
			}
			script := core.TxsToScriptsData(txsData...)
			res, err = l.Execute(context.Background(), true, true, script...)
			require.NoError(b, err)
			require.Len(b, res, 7)
			require.Len(b, res[0].Postings, 1)
			require.Len(b, res[1].Postings, 1)
			require.Len(b, res[2].Postings, 2)
			require.Len(b, res[3].Postings, 4)
			require.Len(b, res[4].Postings, 4)
			require.Len(b, res[5].Postings, 1)
			require.Len(b, res[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes, 7)
		require.Len(b, execRes[0].Postings, 1)
		require.Len(b, execRes[1].Postings, 1)
		require.Len(b, execRes[2].Postings, 2)
		require.Len(b, execRes[3].Postings, 4)
		require.Len(b, execRes[4].Postings, 4)
		require.Len(b, execRes[5].Postings, 1)
		require.Len(b, execRes[6].Postings, 1)
	})
}
