package machine

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name            string
	script          string
	vars            map[string]json.RawMessage
	expectErrorCode error
	expectResult    Result
	store           vm.Store
	metadata        metadata.Metadata
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
				core.NewPosting("world", "user:001", "USD/2", big.NewInt(99)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "not enough funds",
		script: `
			send [USD/2 99] (
				source = @bank
				destination = @user:001
			)`,
		expectErrorCode: vm.ErrInsufficientFund,
	},
	{
		name: "send $0",
		script: `
			send [USD/2 0] (
				source = @alice
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("alice", "user:001", "USD/2", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send $0 world",
		script: `
			send [USD/2 0] (
				source = @world
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "user:001", "USD/2", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
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
				core.NewPosting("alice", "user:001", "USD/2", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
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
				core.NewPosting("world", "user:001", "CAD/2", big.NewInt(42)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
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
				core.NewPosting("world", "bob", "EUR", big.NewInt(1)),
				core.NewPosting("bob", "alice", "EUR", big.NewInt(1)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "using metadata",
		store: vm.StaticStore{
			"sales:001": &core.AccountWithVolumes{
				Account: core.Account{
					Address: "sales:001",
					Metadata: metadata.Metadata{
						"seller": `{
							"type":  "account",
							"value": "users:001"
						}`,
					},
				},
				Volumes: map[string]core.Volumes{
					"COIN": {
						Input:  big.NewInt(100),
						Output: big.NewInt(0),
					},
				},
			},
			"users:001": &core.AccountWithVolumes{
				Account: core.Account{
					Address: "sales:001",
					Metadata: metadata.Metadata{
						"commission": `{
							"type":  "portion",
							"value": "15.5%"
						}`,
					},
				},
				Volumes: map[string]core.Volumes{},
			},
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
				core.NewPosting("sales:001", "users:001", "COIN", big.NewInt(85)),
				core.NewPosting("sales:001", "platform", "COIN", big.NewInt(15)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "defining metadata from input",
		script: `
			send [USD/2 99] (
				source = @world
				destination = @users:001
			)`,
		metadata: metadata.Metadata{
			"priority": "low",
		},
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
			},
			Metadata: metadata.Metadata{
				"priority": "low",
			},
			AccountMetadata: map[string]metadata.Metadata{},
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
				core.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
			},
			Metadata: metadata.Metadata{
				"priority": `{"type":"string","value":"low"}`,
			},
			AccountMetadata: map[string]metadata.Metadata{},
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
		metadata: metadata.Metadata{
			"priority": "low",
		},
		expectErrorCode: vm.ErrMetadataOverride,
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
				core.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
			},
			Metadata: metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{
				"alice": {
					"aaa": `{"type":"string","value":"string meta"}`,
					"bbb": `{"type":"number","value":42}`,
					"ccc": `{"type":"asset","value":"COIN"}`,
					"ddd": `{"type":"monetary","value":{"asset":"COIN","amount":30}}`,
					"eee": `{"type":"account","value":"bob"}`,
				},
			},
		},
	},
	{
		name: "balance function",
		store: vm.StaticStore{
			"users:001": {
				Account: core.Account{
					Address:  "users:001",
					Metadata: metadata.Metadata{},
				},
				Volumes: map[string]core.Volumes{
					"COIN": {
						Input:  big.NewInt(100),
						Output: big.NewInt(0),
					},
				},
			},
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
				core.NewPosting("users:001", "world", "COIN", big.NewInt(100)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
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
				core.NewPosting("users:001", "users:002", "USD/2", big.NewInt(100)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send amount 0",
		store: vm.StaticStore{
			"alice": {
				Account: core.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Volumes: map[string]core.Volumes{},
			},
		},
		script: `
			send [USD 0] (
				source = @alice
				destination = @bob
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("alice", "bob", "USD", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send all with balance 0",
		store: vm.StaticStore{
			"alice": {
				Account: core.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Volumes: map[string]core.Volumes{},
			},
		},
		script: `
			send [USD *] (
				source = @alice
				destination = @bob
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("alice", "bob", "USD", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send account balance of 0",
		store: vm.StaticStore{
			"alice": {
				Account: core.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Volumes: map[string]core.Volumes{},
			},
		},
		script: `
			vars {
				monetary $bal = balance(@alice, USD)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
		expectResult: Result{
			Postings: []core.Posting{
				core.NewPosting("alice", "bob", "USD", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
}

func TestMachine(t *testing.T) {
	t.Parallel()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			if tc.store == nil {
				tc.store = vm.StaticStore{}
			}

			program, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			m := vm.NewMachine(*program)
			require.NoError(t, m.SetVarsFromJSON(tc.vars))

			_, _, err = m.ResolveResources(context.Background(), tc.store)
			require.NoError(t, err)
			require.NoError(t, m.ResolveBalances(context.Background(), tc.store))

			result, err := Run(m, core.RunScript{
				Script: core.Script{
					Plain: tc.script,
					Vars:  tc.vars,
				},
				Metadata: tc.metadata,
			})
			if tc.expectErrorCode != nil {
				require.True(t, errors.Is(err, tc.expectErrorCode))
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tc.expectResult, *result)
			}
		})
	}
}
