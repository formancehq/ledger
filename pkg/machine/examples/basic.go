package main

import (
	"errors"
	"fmt"

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

	m := vm.NewMachine(*program)

	if err = m.SetVars(map[string]core.Value{
		"dest": core.AccountAddress("charlie"),
	}); err != nil {
		panic(err)
	}

	initialBalances := map[string]map[string]*core.MonetaryInt{
		"alice": {"COIN": core.NewMonetaryInt(10)},
		"bob":   {"COIN": core.NewMonetaryInt(100)},
	}

	{
		err := m.ResolveResources(func(acc core.AccountAddress, key string) (*core.Value, error) { return nil, errors.New("a") }, func(acc core.AccountAddress, asset core.Asset) (*core.MonetaryInt, error) {
			return initialBalances[string(acc)][string(asset)], nil
		})
		if err != nil {
			panic(err)
		}
	}

	err = m.Execute()
	if err != nil {
		panic(err)
	}

	fmt.Println("Postings:")
	for _, posting := range m.Postings {
		fmt.Printf("[%v %v] %v -> %v\n", posting.Asset, posting.Amount, posting.Source, posting.Destination)
	}
	fmt.Println("Tx Meta:")
	for key, value := range m.TxMeta {
		fmt.Printf("%v: %v", key, value)
	}
}
