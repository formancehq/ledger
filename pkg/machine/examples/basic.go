package main

import (
	"fmt"
	"math/big"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/script/compiler"
	"github.com/numary/ledger/pkg/machine/vm"
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

	fmt.Printf("%v\n", program)

	// spew.Dump("%#v", program)

	store := vm.StaticStore(map[string]*vm.AccountWithBalances{
		"alice": {
			Account: core.Account{
				Address:  "alice",
				Metadata: map[string]any{},
			},
			Balances: map[string]*big.Int{
				"COIN": big.NewInt(10),
			},
		},
		"bob": {
			Account: core.Account{
				Address:  "bob",
				Metadata: map[string]any{},
			},
			Balances: map[string]*big.Int{
				"COIN": big.NewInt(100),
			},
		},
	})

	m := vm.NewMachine(store)

	err = m.Execute(*program, map[string]string{
		"dest": "charlie",
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Postings:")
	for _, posting := range m.PostingsOuput {
		fmt.Printf("[%v %v] %v -> %v\n", posting.Asset, posting.Amount, posting.Source, posting.Destination)
	}
	fmt.Println("Tx Meta:")
	for key, value := range m.TxMetaOutput {
		fmt.Printf("%v: %v", key, value)
	}
}
