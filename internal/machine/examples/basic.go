package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	vm2 "github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

func main() {
	program, err := compiler.Compile(`
		// This is a comment
		vars {
			account $dest
		}
		send [COIN 99] (
			source = {
				15% from {
					@alice
					@bob
				}
				remaining from @bob
			}
			destination = $dest
		)`)
	if err != nil {
		panic(err)
	}
	fmt.Print(program)

	m := vm2.NewMachine(*program)
	m.Debug = true

	if err = m.SetVarsFromJSON(map[string]string{
		"dest": "charlie",
	}); err != nil {
		panic(err)
	}

	initialVolumes := map[string]map[string]*big.Int{
		"alice": {
			"COIN": big.NewInt(10),
		},
		"bob": {
			"COIN": big.NewInt(100),
		},
	}

	store := vm2.StaticStore{}
	for account, balances := range initialVolumes {
		store[account] = &vm2.AccountWithBalances{
			Account: ledger.Account{
				Address:  account,
				Metadata: metadata.Metadata{},
			},
			Balances: balances,
		}
	}

	_, _, err = m.ResolveResources(context.Background(), vm2.EmptyStore)
	if err != nil {
		panic(err)
	}

	err = m.ResolveBalances(context.Background(), store)
	if err != nil {
		panic(err)
	}

	err = m.Execute()
	if err != nil {
		panic(err)
	}

	fmt.Println(m.Postings)
	fmt.Println(m.TxMeta)
}
