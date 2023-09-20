package machine

import (
	"context"
	"errors"
	"math/big"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	vm2 "github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name            string
	script          string
	vars            map[string]string
	expectErrorCode error
	expectResult    Result
	store           vm2.Store
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
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "user:001", "USD/2", big.NewInt(99)),
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
		expectErrorCode: vm2.ErrInsufficientFund,
	},
	{
		name: "send $0",
		script: `
			send [USD/2 0] (
				source = @alice
				destination = @user:001
			)`,
		expectResult: Result{
			Postings: []ledger.Posting{
				ledger.NewPosting("alice", "user:001", "USD/2", big.NewInt(0)),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "user:001", "USD/2", big.NewInt(0)),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("alice", "user:001", "USD/2", big.NewInt(0)),
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
		vars: map[string]string{
			"dest": "user:001",
		},
		expectResult: Result{
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "user:001", "CAD/2", big.NewInt(42)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "using metadata",
		store: vm2.StaticStore{
			"sales:001": &vm2.AccountWithBalances{
				Account: ledger.Account{
					Address: "sales:001",
					Metadata: metadata.Metadata{
						"seller": "users:001",
					},
				},
				Balances: map[string]*big.Int{
					"COIN": big.NewInt(100),
				},
			},
			"users:001": &vm2.AccountWithBalances{
				Account: ledger.Account{
					Address: "sales:001",
					Metadata: metadata.Metadata{
						"commission": "15.5%",
					},
				},
				Balances: map[string]*big.Int{},
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
		vars: map[string]string{
			"sale": "sales:001",
		},
		expectResult: Result{
			Postings: []ledger.Posting{
				ledger.NewPosting("sales:001", "users:001", "COIN", big.NewInt(85)),
				ledger.NewPosting("sales:001", "platform", "COIN", big.NewInt(15)),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
			},
			Metadata: metadata.Metadata{
				"priority": "low",
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
		expectErrorCode: vm2.ErrMetadataOverride,
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
			Postings: []ledger.Posting{
				ledger.NewPosting("world", "users:001", "USD/2", big.NewInt(99)),
			},
			Metadata: metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{
				"alice": {
					"aaa": "string meta",
					"bbb": "42",
					"ccc": "COIN",
					"ddd": "COIN 30",
					"eee": "bob",
				},
			},
		},
	},
	{
		name: "balance function",
		store: vm2.StaticStore{
			"users:001": {
				Account: ledger.Account{
					Address:  "users:001",
					Metadata: metadata.Metadata{},
				},
				Balances: map[string]*big.Int{
					"COIN": big.NewInt(100),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("users:001", "world", "COIN", big.NewInt(100)),
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
			Postings: []ledger.Posting{
				ledger.NewPosting("users:001", "users:002", "USD/2", big.NewInt(100)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send amount 0",
		store: vm2.StaticStore{
			"alice": {
				Account: ledger.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Balances: map[string]*big.Int{},
			},
		},
		script: `
			send [USD 0] (
				source = @alice
				destination = @bob
			)`,
		expectResult: Result{
			Postings: []ledger.Posting{
				ledger.NewPosting("alice", "bob", "USD", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send all with balance 0",
		store: vm2.StaticStore{
			"alice": {
				Account: ledger.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Balances: map[string]*big.Int{},
			},
		},
		script: `
			send [USD *] (
				source = @alice
				destination = @bob
			)`,
		expectResult: Result{
			Postings: []ledger.Posting{
				ledger.NewPosting("alice", "bob", "USD", big.NewInt(0)),
			},
			Metadata:        metadata.Metadata{},
			AccountMetadata: map[string]metadata.Metadata{},
		},
	},
	{
		name: "send account balance of 0",
		store: vm2.StaticStore{
			"alice": {
				Account: ledger.Account{
					Address:  "alice",
					Metadata: metadata.Metadata{},
				},
				Balances: map[string]*big.Int{},
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
			Postings: []ledger.Posting{
				ledger.NewPosting("alice", "bob", "USD", big.NewInt(0)),
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
				tc.store = vm2.StaticStore{}
			}

			program, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			m := vm2.NewMachine(*program)
			require.NoError(t, m.SetVarsFromJSON(tc.vars))

			_, _, err = m.ResolveResources(context.Background(), tc.store)
			require.NoError(t, err)
			require.NoError(t, m.ResolveBalances(context.Background(), tc.store))

			result, err := Run(m, ledger.RunScript{
				Script: ledger.Script{
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
