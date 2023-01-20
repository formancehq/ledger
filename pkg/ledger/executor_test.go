package ledger_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestNoScript(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{}

		_, err := l.Execute(context.Background(), script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorNoScript, err.(*ledger.ScriptError).Code)
	})
}

func TestCompilationError(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{
			Plain: "willnotcompile",
		}

		_, err := l.Execute(context.Background(), script)
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorCompilationFailed, err.(*ledger.ScriptError).Code)
	})
}

func TestTransactionInvalidScript(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{
			Plain: "this is not a valid script",
		}

		_, err := l.Execute(context.Background(), script)
		assert.Error(t, err, "script was invalid yet the transaction was committed")

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestTransactionFail(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.Script{
			Plain: "fail",
		}

		_, err := l.Execute(context.Background(), script)
		assert.Error(t, err, "script failed yet the transaction was committed")

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestSend(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		script := core.Script{
			Plain: `send [USD/2 99] (
				source=@world
				destination=@user:001
			)`,
		}

		_, err := l.Execute(context.Background(), script)
		require.NoError(t, err)

		assertBalance(t, l, "user:001", "USD/2", core.NewMonetaryInt(99))
	})
}

func TestNoVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		var script core.Script
		err := json.Unmarshal(
			[]byte(`{
				"plain": "vars {\naccount $dest\n}\nsend [CAD/2 42] (\n source=@world \n destination=$dest \n)",
				"vars": {}
			}`),
			&script)
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), script)
		assert.Error(t, err, "variables were not provided but the transaction was committed")

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestVariables(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

		var script core.Script
		err := json.Unmarshal(
			[]byte(`{
				"plain": "vars {\naccount $dest\n}\nsend [CAD/2 42] (\n source=@world \n destination=$dest \n)",
				"vars": {
					"dest": "user:042"
				}
			}`),
			&script)
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), script)
		require.NoError(t, err)

		user, err := l.GetAccount(context.Background(), "user:042")
		require.NoError(t, err)

		b := user.Balances["CAD/2"]
		assert.Equalf(t, core.NewMonetaryInt(42), b,
			"wrong CAD/2 balance for account user:042, expected: %d got: %d",
			42, b,
		)
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

		_, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		var script core.Script
		err = json.Unmarshal(
			[]byte(`{
				"plain": "send [COIN 95] (\n source=@user:001 \n destination=@world \n)"
			}`),
			&script)
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), script)
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

		_, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		var script core.Script
		err = json.Unmarshal(
			[]byte(`{
				"plain": "send [COIN 105] (\n source=@user:002 \n destination=@world \n)"
			}`),
			&script)
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), script)
		assert.Error(t, err, "error wasn't supposed to be nil")
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
			)
		`

		script := core.Script{
			Plain: plain,
			Vars: map[string]json.RawMessage{
				"sale": json.RawMessage(`"sales:042"`),
			},
		}

		_, err := l.Execute(context.Background(), script)
		assert.Error(t, err, "expected an error because of missing metadata")
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

		_, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "sales:042", core.Metadata{
			"seller": json.RawMessage(`{
				"type":  "account",
				"value": "users:053"
			}`),
		})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:053", core.Metadata{
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
			Plain: plain,
			Vars: map[string]json.RawMessage{
				"sale": json.RawMessage(`"sales:042"`),
			},
		}

		_, err = l.Execute(context.Background(), script)
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
				Plain: `send [USD/2 99] (
					source=@world
					destination=@user:001
				)`,
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
				Plain: `
                set_tx_meta("priority", "low")
                send [COIN 10] (
                    source = @world
                    destination = @user:001
				)`,
			},
			expectedMetadata: core.Metadata{
				"priority": map[string]any{"type": "string", "value": "low"},
			},
		},
		{
			name: "override metadata of script",
			script: core.Script{
				Plain: `
				set_tx_meta("priority", "low")

				send [USD/2 99] (
					source=@world
					destination=@user:001
				)`,
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

				_, err := l.Execute(context.Background(), tc.script)

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

		plain := `send [USD/2 99] (
			source=@world
			destination=@user:001
		)`

		script := core.Script{
			Plain:     plain,
			Vars:      map[string]json.RawMessage{},
			Reference: "tx_ref",
		}

		_, err := l.Execute(context.Background(), script)
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

		_, err := l.Execute(context.Background(), core.Script{
			Plain: `
				send [USD/2 99] (
					source=@world
					destination=@user:001
				)`,
			Vars:      map[string]json.RawMessage{},
			Reference: "tx_ref",
		})
		require.NoError(t, err)

		_, err = l.Execute(context.Background(), core.Script{
			Plain: `
				send [USD/2 99] (
					source=@unexists
					destination=@user:001
				)`,
			Vars:      map[string]json.RawMessage{},
			Reference: "tx_ref",
		})
		require.Error(t, err)
		require.True(t, ledger.IsConflictError(err))
	})
}

var execRes *ledger.CommitResult

func BenchmarkLedger_PostTransactions(b *testing.B) {
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

		res := &ledger.CommitResult{}

		for n := 0; n < b.N; n++ {
			var err error
			res, err = l.CommitPreview(context.Background(), []core.TransactionData{txData})
			require.NoError(b, err)
			require.Len(b, res.GeneratedTransactions, 1)
			require.Len(b, res.GeneratedTransactions[0].Postings, 1000)
		}

		execRes = res
		require.Len(b, execRes.GeneratedTransactions, 1)
		require.Len(b, execRes.GeneratedTransactions[0].Postings, 1000)
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
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(b, l.Close(ctx))
		}(l, context.Background())

		txsData := newTxsData(1)

		b.ResetTimer()

		res := &ledger.CommitResult{}

		for n := 0; n < b.N; n++ {
			var err error
			res, err = l.CommitPreview(context.Background(), txsData)
			require.NoError(b, err)
			require.Len(b, res.GeneratedTransactions, 7)
			require.Len(b, res.GeneratedTransactions[0].Postings, 1)
			require.Len(b, res.GeneratedTransactions[1].Postings, 1)
			require.Len(b, res.GeneratedTransactions[2].Postings, 2)
			require.Len(b, res.GeneratedTransactions[3].Postings, 4)
			require.Len(b, res.GeneratedTransactions[4].Postings, 4)
			require.Len(b, res.GeneratedTransactions[5].Postings, 1)
			require.Len(b, res.GeneratedTransactions[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes.GeneratedTransactions, 7)
		require.Len(b, execRes.GeneratedTransactions[0].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[1].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[2].Postings, 2)
		require.Len(b, execRes.GeneratedTransactions[3].Postings, 4)
		require.Len(b, execRes.GeneratedTransactions[4].Postings, 4)
		require.Len(b, execRes.GeneratedTransactions[5].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[6].Postings, 1)
	})
}

func BenchmarkLedger_PostTransactionsBatch2(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(b, l.Close(ctx))
		}(l, context.Background())

		b.ResetTimer()

		res := &ledger.CommitResult{}

		for n := 0; n < b.N; n++ {
			b.StopTimer()
			txsData := newTxsData(n)
			b.StartTimer()
			var err error
			res, err = l.CommitPreview(context.Background(), txsData)
			require.NoError(b, err)
			require.Len(b, res.GeneratedTransactions, 7)
			require.Len(b, res.GeneratedTransactions[0].Postings, 1)
			require.Len(b, res.GeneratedTransactions[1].Postings, 1)
			require.Len(b, res.GeneratedTransactions[2].Postings, 2)
			require.Len(b, res.GeneratedTransactions[3].Postings, 4)
			require.Len(b, res.GeneratedTransactions[4].Postings, 4)
			require.Len(b, res.GeneratedTransactions[5].Postings, 1)
			require.Len(b, res.GeneratedTransactions[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes.GeneratedTransactions, 7)
		require.Len(b, execRes.GeneratedTransactions[0].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[1].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[2].Postings, 2)
		require.Len(b, execRes.GeneratedTransactions[3].Postings, 4)
		require.Len(b, execRes.GeneratedTransactions[4].Postings, 4)
		require.Len(b, execRes.GeneratedTransactions[5].Postings, 1)
		require.Len(b, execRes.GeneratedTransactions[6].Postings, 1)
	})
}
