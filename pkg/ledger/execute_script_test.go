package ledger_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoScript(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		script := core.ScriptData{}

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		err := waitAndPostProcess(context.Background())
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorNoScript, err.(*ledger.ScriptError).Code)
	})
}

func TestCompilationError(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		script := core.ScriptData{
			Script: core.Script{Plain: "willnotcompile"},
		}

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		err := waitAndPostProcess(context.Background())
		assert.IsType(t, &ledger.ScriptError{}, err)
		assert.Equal(t, ledger.ScriptErrorCompilationFailed, err.(*ledger.ScriptError).Code)
	})
}

func TestMappingIgnoreDestinations(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		script := core.ScriptData{
			Script: core.Script{
				Plain: `
					send [USD/2 1100] (
						source = @A allowing overdraft up to [USD/2 1100]
						destination = @B
					)`,
			},
		}
		_, err := l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, err)

		_, err = l.ExecuteTxsData(context.Background(), false, core.TransactionData{
			Postings: []core.Posting{{
				Source:      "B",
				Destination: "A",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD/2",
			}},
		})
		require.NoError(t, err)

		_, err = l.ExecuteTxsData(context.Background(), false, core.TransactionData{
			Postings: []core.Posting{{
				Source:      "B",
				Destination: "A",
				Amount:      core.NewMonetaryInt(0),
				Asset:       "USD/2",
			}},
		})
		require.NoError(t, err)
	})
}

func TestSend(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		t.Run("nominal", func(t *testing.T) {
			script := core.ScriptData{
				Script: core.Script{
					Plain: `
					send [USD/2 99] (
						source = @world
						destination = @user:001
					)`,
				},
			}
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			require.NoError(t, waitAndPostProcess(context.Background()))

			assertBalance(t, l, "user:001",
				"USD/2", core.NewMonetaryInt(99))
		})

		t.Run("one send with zero amount should fail", func(t *testing.T) {
			script := core.ScriptData{
				Script: core.Script{
					Plain: `
					send [USD/2 0] (
						source = @world
						destination = @user:001
					)`,
				},
			}
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			err := waitAndPostProcess(context.Background())
			require.Error(t, err)
			require.True(t, ledger.IsValidationError(err))
			require.ErrorContains(t, err, "transaction has no postings")
		})

		t.Run("one send with monetary all should fail", func(t *testing.T) {
			script := core.ScriptData{
				Script: core.Script{
					Plain: `
					send [USD/2 *] (
						source = @alice
						destination = @user:001
					)`,
				},
			}
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			err := waitAndPostProcess(context.Background())
			require.Error(t, err)
			require.True(t, ledger.IsValidationError(err))
			require.ErrorContains(t, err, "transaction has no postings")
		})

		t.Run("one send with zero amount and another with positive amount should succeed", func(t *testing.T) {
			script := core.ScriptData{
				Script: core.Script{
					Plain: `
					send [USD/2 0] (
						source = @world
						destination = @user:001
					)
					send [USD/2 1] (
						source = @world
						destination = @user:001
					)`,
				},
			}
			res, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			require.NoError(t, waitAndPostProcess(context.Background()))
			require.Equal(t, 1, len(res.Postings))

			assertBalance(t, l, "user:001",
				"USD/2", core.NewMonetaryInt(100))
		})

		t.Run("one send with monetary all and another with positive amount should succeed", func(t *testing.T) {
			script := core.ScriptData{
				Script: core.Script{
					Plain: `
					send [USD/2 *] (
						source = @alice
						destination = @user:001
					)
					send [USD/2 1] (
						source = @world
						destination = @user:001
					)`,
				},
			}
			res, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			require.NoError(t, waitAndPostProcess(context.Background()))
			require.Equal(t, 1, len(res.Postings))

			assertBalance(t, l, "user:001",
				"USD/2", core.NewMonetaryInt(101))
		})
	})
}

func TestNoVariables(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
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

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		assert.Error(t, waitAndPostProcess(context.Background()))
	})
}

func TestVariables(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
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

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, waitAndPostProcess(context.Background()))

		assertBalance(t, l, "user:042",
			"CAD/2", core.NewMonetaryInt(42))
	})
}

func TestVariablesEmptyAccount(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		script := core.ScriptData{
			Script: core.Script{
				Plain: `
					send [EUR 1] (
						source = @world
						destination = @bob
					)`,
			},
		}
		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, waitAndPostProcess(context.Background()))

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
		_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, waitAndPostProcess(context.Background()))

		assertBalance(t, l, "alice", "EUR", core.NewMonetaryInt(1))
		assertBalance(t, l, "bob", "EUR", core.NewMonetaryInt(0))
	})
}

func TestEnoughFunds(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
			Script: core.Script{
				Plain: `send [COIN 100] (
	source = @world
	destination = @user:001
)`,
			},
			Timestamp: time.Time{},
		})
		require.NoError(t, waitAndPostProcess(context.Background()))

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
 					send [COIN 95] (
 						source = @user:001
 						destination = @world
 					)`,
			},
		}

		_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
		assert.NoError(t, waitAndPostProcess(context.Background()))
	})
}

func TestNotEnoughFunds(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
			Script: core.Script{
				Plain: `send [COIN 100] (
	source = @world
	destination = @user:002
)`,
			},
			Timestamp: time.Time{},
		})
		require.NoError(t, waitAndPostProcess(context.Background()))

		script := core.ScriptData{
			Script: core.Script{
				Plain: `
 					send [COIN 105] (
 						source = @user:002
 						destination = @world
 					)`,
			},
		}

		_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
		assert.True(t, ledger.IsScriptErrorWithCode(waitAndPostProcess(context.Background()), apierrors.ErrInsufficientFund))
	})
}

func TestMissingMetadata(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
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

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		assert.True(t, ledger.IsScriptErrorWithCode(waitAndPostProcess(context.Background()), ledger.ScriptErrorCompilationFailed))
	})
}

func TestMetadata(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
			Script: core.Script{
				Plain: `send [COIN 100] (
	source = @world
	destination = @sales:042
)`,
			},
			Timestamp: time.Time{},
		})
		require.NoError(t, waitAndPostProcess(context.Background()))

		waitAndPostProcess = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount,
			"sales:042",
			core.Metadata{
				"seller": json.RawMessage(`{
 					"type":  "account",
 					"value": "users:053"
 				}`),
			})
		require.NoError(t, waitAndPostProcess(context.Background()))

		waitAndPostProcess = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount,
			"users:053",
			core.Metadata{
				"commission": json.RawMessage(`{
 					"type":  "portion",
 					"value": "15.5%"
 				}`),
			})
		require.NoError(t, waitAndPostProcess(context.Background()))

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

		script := core.ScriptData{
			Script: core.Script{
				Plain: plain,
				Vars: map[string]json.RawMessage{
					"sale": json.RawMessage(`"sales:042"`),
				},
			},
		}

		_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, waitAndPostProcess(context.Background()))

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
			runOnLedger(t, func(l *ledger.Ledger) {
				_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, tc.script)
				err := waitAndPostProcess(context.Background())

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
	runOnLedger(t, func(l *ledger.Ledger) {
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

		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
		require.NoError(t, waitAndPostProcess(context.Background()))

		last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		assert.Equal(t, script.Reference, last.Reference)
	})
}

func TestScriptReferenceConflict(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, waitAndPostProcess := l.ExecuteScript(context.Background(), false,
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
		require.NoError(t, waitAndPostProcess(context.Background()))

		_, waitAndPostProcess = l.ExecuteScript(context.Background(), false,
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
		err := waitAndPostProcess(context.Background())
		require.Error(t, err)
		require.True(t, ledger.IsConflictError(err))
	})
}

func TestSetAccountMeta(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		t.Run("valid", func(t *testing.T) {
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false,
				core.ScriptData{
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
			require.NoError(t, waitAndPostProcess(context.Background()))

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
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false,
				core.ScriptData{
					Script: core.Script{Plain: `
						send [USD/2 99] (
							source = @world
							destination = @user:001
						)
						set_account_meta(@bob, "is")
					`},
				})
			require.True(t, ledger.IsScriptErrorWithCode(waitAndPostProcess(context.Background()),
				ledger.ScriptErrorCompilationFailed))
		})
	})
}

func TestMonetaryVariableBalance(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
				Script: core.Script{
					Plain: `send [COIN 100] (
	source = @world
	destination = @users:001
)`,
				},
				Timestamp: time.Time{},
			})
			require.NoError(t, waitAndPostProcess(context.Background()))

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

			_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
			require.NoError(t, waitAndPostProcess(context.Background()))
			assertBalance(t, l, "world", "COIN", core.NewMonetaryInt(0))
			assertBalance(t, l, "users:001", "COIN", core.NewMonetaryInt(0))
		})
	})

	t.Run("complex", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
				Script: core.Script{
					Plain: `
send [USD/2 40] (
	source = @world
	destination = @A
)
send [USD/2 90] (
	source = @world
	destination = @C
)
`,
				},
				Timestamp: time.Time{},
			})
			require.NoError(t, waitAndPostProcess(context.Background()))

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

			_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
			require.NoError(t, waitAndPostProcess(context.Background()))
			assertBalance(t, l, "B", "USD/2", core.NewMonetaryInt(40))
			assertBalance(t, l, "D", "USD/2", core.NewMonetaryInt(60))
		})
	})

	t.Run("error insufficient funds", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
				Script: core.Script{
					Plain: `send [COIN 100] (
	source = @world
	destination = @users:001
)`,
				},
				Timestamp: time.Time{},
			})
			require.NoError(t, waitAndPostProcess(context.Background()))

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
			_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
			assert.True(t, ledger.IsScriptErrorWithCode(waitAndPostProcess(context.Background()), apierrors.ErrInsufficientFund))
		})
	})

	t.Run("error negative balance", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, core.ScriptData{
				Script: core.Script{
					Plain: `send [COIN 100] (
	source = @world
	destination = @users:001
)`,
				},
				Timestamp: time.Time{},
			})
			require.NoError(t, waitAndPostProcess(context.Background()))

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

			_, waitAndPostProcess = l.ExecuteScript(context.Background(), false, script)
			err := waitAndPostProcess(context.Background())
			assert.True(t, ledger.IsScriptErrorWithCode(err, ledger.ScriptErrorCompilationFailed))
			assert.ErrorContains(t, err, "must be non-negative")
		})
	})

	t.Run("error variable type", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
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
			_, waitAndPostProcess := l.ExecuteScript(context.Background(), false, script)
			assert.True(t, ledger.IsScriptErrorWithCode(waitAndPostProcess(context.Background()), apierrors.ErrScriptCompilationFailed))
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

func TestNewMachineFromScript(t *testing.T) {
	_, span := opentelemetry.Start(context.Background(), "TestNewMachineFromScript")
	defer span.End()

	txData := core.TransactionData{}
	for i := 0; i < nbPostings; i++ {
		txData.Postings = append(txData.Postings, core.Posting{
			Source:      "world",
			Destination: "benchmarks:" + strconv.Itoa(i),
			Asset:       "COIN",
			Amount:      core.NewMonetaryInt(10),
		})
	}
	_, err := txData.Postings.Validate()
	require.NoError(t, err)
	script := txToScriptData(txData)

	h := sha256.New()
	_, err = h.Write([]byte(script.Plain))
	require.NoError(t, err)
	key := h.Sum(nil)
	keySizeBytes := size.Of(key)
	require.NotEqual(t, -1, keySizeBytes)

	prog, err := compiler.Compile(script.Plain)
	require.NoError(t, err)
	progSizeBytes := size.Of(*prog)
	require.NotEqual(t, -1, progSizeBytes)

	t.Run("exact size", func(t *testing.T) {
		capacityBytes := int64(keySizeBytes + progSizeBytes)

		cache := ledger.NewCache(capacityBytes, 1, true)

		m, err := ledger.NewMachineFromScript(script.Plain, cache, span)
		require.NoError(t, err)
		require.NotNil(t, m)
		cache.Wait()
		require.Equal(t, uint64(0), cache.Metrics.Hits())
		require.Equal(t, uint64(1), cache.Metrics.Misses())
		require.Equal(t, uint64(1), cache.Metrics.KeysAdded())

		m, err = ledger.NewMachineFromScript(script.Plain, cache, span)
		require.NoError(t, err)
		require.NotNil(t, m)
		cache.Wait()
		require.Equal(t, uint64(1), cache.Metrics.Hits())
		require.Equal(t, uint64(1), cache.Metrics.Misses())
		require.Equal(t, uint64(1), cache.Metrics.KeysAdded())
	})

	t.Run("one byte too small", func(t *testing.T) {
		capacityBytes := int64(keySizeBytes+progSizeBytes) - 1

		cache := ledger.NewCache(capacityBytes, 1, true)

		m, err := ledger.NewMachineFromScript(script.Plain, cache, span)
		require.NoError(t, err)
		require.NotNil(t, m)
		cache.Wait()
		require.Equal(t, uint64(0), cache.Metrics.Hits())
		require.Equal(t, uint64(1), cache.Metrics.Misses())
		require.Equal(t, uint64(0), cache.Metrics.KeysAdded())

		m, err = ledger.NewMachineFromScript(script.Plain, cache, span)
		require.NoError(t, err)
		require.NotNil(t, m)
		cache.Wait()
		require.Equal(t, uint64(0), cache.Metrics.Hits())
		require.Equal(t, uint64(2), cache.Metrics.Misses())
		require.Equal(t, uint64(0), cache.Metrics.KeysAdded())
	})
}

type variable struct {
	name    string
	jsonVal json.RawMessage
}

func txToScriptData(txData core.TransactionData) core.ScriptData {
	if len(txData.Postings) == 0 {
		return core.ScriptData{}
	}

	sb := strings.Builder{}
	monetaryToVars := map[string]variable{}
	accountsToVars := map[string]variable{}
	i := 0
	j := 0
	for _, p := range txData.Postings {
		if _, ok := accountsToVars[p.Source]; !ok {
			if p.Source != core.WORLD {
				accountsToVars[p.Source] = variable{
					name:    fmt.Sprintf("va%d", i),
					jsonVal: json.RawMessage(`"` + p.Source + `"`),
				}
				i++
			}
		}
		if _, ok := accountsToVars[p.Destination]; !ok {
			if p.Destination != core.WORLD {
				accountsToVars[p.Destination] = variable{
					name:    fmt.Sprintf("va%d", i),
					jsonVal: json.RawMessage(`"` + p.Destination + `"`),
				}
				i++
			}
		}
		mon := fmt.Sprintf("[%s %s]", p.Amount.String(), p.Asset)
		if _, ok := monetaryToVars[mon]; !ok {
			monetaryToVars[mon] = variable{
				name: fmt.Sprintf("vm%d", j),
				jsonVal: json.RawMessage(
					`{"asset":"` + p.Asset + `","amount":` + p.Amount.String() + `}`),
			}
			j++
		}
	}

	sb.WriteString("vars {\n")
	accVars := make([]string, 0)
	for _, v := range accountsToVars {
		accVars = append(accVars, v.name)
	}
	sort.Strings(accVars)
	for _, v := range accVars {
		sb.WriteString(fmt.Sprintf("\taccount $%s\n", v))
	}
	monVars := make([]string, 0)
	for _, v := range monetaryToVars {
		monVars = append(monVars, v.name)
	}
	sort.Strings(monVars)
	for _, v := range monVars {
		sb.WriteString(fmt.Sprintf("\tmonetary $%s\n", v))
	}
	sb.WriteString("}\n")

	for _, p := range txData.Postings {
		m := fmt.Sprintf("[%s %s]", p.Amount.String(), p.Asset)
		mon, ok := monetaryToVars[m]
		if !ok {
			panic(fmt.Sprintf("monetary %s not found", m))
		}
		sb.WriteString(fmt.Sprintf("send $%s (\n", mon.name))
		if p.Source == core.WORLD {
			sb.WriteString("\tsource = @world\n")
		} else {
			src, ok := accountsToVars[p.Source]
			if !ok {
				panic(fmt.Sprintf("source %s not found", p.Source))
			}
			sb.WriteString(fmt.Sprintf("\tsource = $%s allowing unbounded overdraft\n", src.name))
		}
		if p.Destination == core.WORLD {
			sb.WriteString("\tdestination = @world\n")
		} else {
			dest, ok := accountsToVars[p.Destination]
			if !ok {
				panic(fmt.Sprintf("destination %s not found", p.Destination))
			}
			sb.WriteString(fmt.Sprintf("\tdestination = $%s\n", dest.name))
		}
		sb.WriteString(")\n")
	}

	vars := map[string]json.RawMessage{}
	for _, v := range accountsToVars {
		vars[v.name] = v.jsonVal
	}
	for _, v := range monetaryToVars {
		vars[v.name] = v.jsonVal
	}

	return core.ScriptData{
		Script: core.Script{
			Plain: sb.String(),
			Vars:  vars,
		},
		Timestamp: txData.Timestamp,
		Reference: txData.Reference,
		Metadata:  txData.Metadata,
	}
}
