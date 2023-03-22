package machine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name            string
	script          string
	vars            map[string]json.RawMessage
	expectErrorCode string
	expectResult    Result
	setup           func(t *testing.T, store storage.LedgerStore)
	metadata        core.Metadata
}

var testCases = []testCase{
	{
		name: "nominal",
		script: `
			send [USD/2 99] (
				source = @world
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "user:001", "USD/2", core.NewMonetaryInt(99)),
			},
			Metadata:        map[string]any{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "not enough funds",
		script: `
			send [USD/2 99] (
				source = @bank
				destination = @user:001
			)`,
		expectErrorCode: vm.ScriptErrorInsufficientFund,
	},
	{
		name: "send 0$",
		script: `
			send [USD/2 0] (
				source = @world
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				// TODO: The machine should return a posting with 0 as amount
				//core.NewPosting("world", "user:001", "USD/2", core.NewMonetaryInt(0)),
			},
			Metadata:        map[string]any{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "send all available",
		script: `
			send [USD/2 *] (
				source = @alice
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				// TODO: The machine should return a posting with 0 as amount
				//core.NewPosting("world", "user:001", "USD/2", core.NewMonetaryInt(0)),
			},
			Metadata:        map[string]any{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "with variable",
		script: `
			vars {
				account $dest
			}

			send [CAD/2 42] (
				source = @world
				destination = $dest
			)`,
		vars: map[string]json.RawMessage{
			"dest": json.RawMessage(`"user:001"`),
		},
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "user:001", "CAD/2", core.NewMonetaryInt(42)),
			},
			Metadata:        map[string]any{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "missing variable value",
		script: `
			vars {
				account $dest
			}

			send [CAD/2 42] (
				source = @world
				destination = $dest
			)`,
		vars:            map[string]json.RawMessage{},
		expectErrorCode: vm.ScriptErrorCompilationFailed,
	},
	{
		name: "use empty account",
		script: `
			vars {
				account $acc
			}

			send [EUR 1] (
				source = @world
				destination = @bob
			)

			send [EUR 1] (
				source = {
					@bob
					$acc
				}
				destination = @alice
			)`,
		vars: map[string]json.RawMessage{
			"acc": json.RawMessage(`""`),
		},
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "bob", "EUR", core.NewMonetaryInt(1)),
				core.NewPosting("bob", "alice", "EUR", core.NewMonetaryInt(1)),
			},
			Metadata:        map[string]any{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "missing metadata",
		script: `
			vars {
				account $sale
				account $seller = meta($sale, "seller")
			}

			send [COIN *] (
				source = $sale
				destination = $seller
			)`,
		vars: map[string]json.RawMessage{
			"sale": json.RawMessage(`"sales:042"`),
		},
		expectErrorCode: vm.ScriptErrorCompilationFailed,
	},
	{
		name: "using metadata",
		setup: func(t *testing.T, store storage.LedgerStore) {
			require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
				"sales:001": {
					"COIN": {
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
					},
				},
			}))
			require.NoError(t, store.UpdateAccountMetadata(context.Background(), "sales:001", core.Metadata{
				"seller": json.RawMessage(`{
					"type":  "account",
					"value": "users:001"
				}`),
			}))
			require.NoError(t, store.UpdateAccountMetadata(context.Background(), "users:001", core.Metadata{
				"commission": json.RawMessage(`{
					"type":  "portion",
					"value": "15.5%"
				}`),
			}))
		},
		script: `
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
		`,
		vars: map[string]json.RawMessage{
			"sale": json.RawMessage(`"sales:001"`),
		},
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("sales:001", "users:001", "COIN", core.NewMonetaryInt(85)),
				core.NewPosting("sales:001", "platform", "COIN", core.NewMonetaryInt(15)),
			},
			Metadata:        core.Metadata{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "defining metadata from input",
		script: `
			send [USD/2 99] (
				source = @world
				destination = @users:001
			)`,
		metadata: core.Metadata{
			"priority": "low",
		},
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "users:001", "USD/2", core.NewMonetaryInt(99)),
			},
			Metadata: core.Metadata{
				"priority": "low",
			},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "defining metadata from script",
		script: `
			set_tx_meta("priority", "low")
			send [USD/2 99] (
				source = @world
				destination = @users:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "users:001", "USD/2", core.NewMonetaryInt(99)),
			},
			Metadata: core.Metadata{
				"priority": map[string]any{
					"type":  "string",
					"value": "low",
				},
			},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "override metadata from script",
		script: `
			set_tx_meta("priority", "low")
			send [USD/2 99] (
				source = @world
				destination = @users:001
			)`,
		metadata: core.Metadata{
			"priority": "low",
		},
		expectErrorCode: vm.ScriptErrorMetadataOverride,
	},
	{
		name: "set account meta",
		script: `
			send [USD/2 99] (
				source = @world
				destination = @users:001
			)
			set_account_meta(@alice, "aaa", "string meta")
			set_account_meta(@alice, "bbb", 42)
			set_account_meta(@alice, "ccc", COIN)
			set_account_meta(@alice, "ddd", [COIN 30])
			set_account_meta(@alice, "eee", @bob)
		`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "users:001", "USD/2", core.NewMonetaryInt(99)),
			},
			Metadata: core.Metadata{},
			AccountMetadata: map[string]core.Metadata{
				"alice": {
					"aaa": map[string]any{"type": "string", "value": "string meta"},
					"bbb": map[string]any{"type": "number", "value": 42.},
					"ccc": map[string]any{"type": "asset", "value": "COIN"},
					"ddd": map[string]any{"type": "monetary", "value": map[string]any{"asset": "COIN", "amount": 30.}},
					"eee": map[string]any{"type": "account", "value": "bob"},
				},
			},
		},
	},
	{
		name: "balance function",
		setup: func(t *testing.T, store storage.LedgerStore) {
			require.NoError(t, store.EnsureAccountExists(context.Background(), "users:001"))
			require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
				"users:001": map[string]core.Volumes{
					"COIN": {
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
					},
				},
			}))
		},
		script: `
			vars {
				monetary $bal = balance(@users:001, COIN)
			}
			send $bal (
				source = @users:001
				destination = @world
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("users:001", "world", "COIN", core.NewMonetaryInt(100)),
			},
			Metadata:        core.Metadata{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
	{
		name: "balance function with negative balance",
		setup: func(t *testing.T, store storage.LedgerStore) {
			require.NoError(t, store.EnsureAccountExists(context.Background(), "users:001"))
			require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
				"users:001": map[string]core.Volumes{
					"COIN": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(100),
					},
				},
			}))
		},
		script: `
			vars {
				monetary $bal = balance(@users:001, COIN)
			}
			send $bal (
				source = @users:001
				destination = @world
			)`,
		expectErrorCode: vm.ScriptErrorCompilationFailed,
	},
	{
		name: "overdraft",
		script: `
		send [USD/2 100] (
		  	source = @users:001 allowing unbounded overdraft
		  	destination = @users:002
		)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("users:001", "users:002", "USD/2", core.NewMonetaryInt(100)),
			},
			Metadata:        core.Metadata{},
			AccountMetadata: map[string]core.Metadata{},
		},
	},
}

func TestMachine(t *testing.T) {
	t.Parallel()

	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		_ = pgtesting.DestroyPostgresServer()
	}()

	storageDriver := ledgertesting.StorageDriver(t)
	require.NoError(t, storageDriver.Initialize(context.Background()))

	cacheManager := cache.NewManager(storageDriver)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ledger := uuid.NewString()

			cache, err := cacheManager.ForLedger(context.Background(), ledger)
			require.NoError(t, err)

			store, _, err := storageDriver.GetLedgerStore(context.Background(), ledger, true)
			require.NoError(t, err)

			_, err = store.Initialize(context.Background())
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, store)
			}

			prog, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			result, err := Run(context.Background(), cache, prog, core.RunScript{
				Script: core.Script{
					Plain: tc.script,
					Vars:  tc.vars,
				},
				Metadata: tc.metadata,
			})
			if tc.expectErrorCode != "" {
				require.True(t, vm.IsScriptErrorWithCode(err, tc.expectErrorCode))
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tc.expectResult, *result)
			}
		})
	}
}
