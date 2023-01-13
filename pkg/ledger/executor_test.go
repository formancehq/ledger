package ledger_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

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

func BenchmarkLedger_Post(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(b, l.Close(ctx))
		}(l, context.Background())

		txData := core.TransactionData{}
		for i := 0; i < 1000; i++ {
			txData.Postings = append(txData.Postings, core.Posting{
				Source:      "world",
				Destination: "benchmarks:" + strconv.Itoa(i),
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(10),
			})
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := txData.Postings.Validate()
			require.NoError(b, err)
			script := core.TxsToScriptsData(txData)
			res, err := l.Execute(context.Background(), true, true, script...)
			require.NoError(b, err)
			require.Len(b, res, 1)
			require.Len(b, res.GeneratedTransactions[0].Postings, 1000)
		}
	})
}
