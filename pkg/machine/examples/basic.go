package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/internal"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm"
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

	m := vm.NewMachine(*program)
	m.Debug = true

	if err = m.SetVars(map[string]internal.Value{
		"dest": internal.AccountAddress("charlie"),
	}); err != nil {
		panic(err)
	}

	initialVolumes := map[string]map[string]core.Volumes{
		"alice": {
			"COIN": core.NewEmptyVolumes().WithInput(big.NewInt(10)),
		},
		"bob": {
			"COIN": core.NewEmptyVolumes().WithInput(big.NewInt(100)),
		},
	}

	_, _, err = m.ResolveResources(context.Background(), vm.EmptyStore)
	if err != nil {
		panic(err)
	}

	err = m.ResolveBalances(context.Background(), vm.StoreFn(func(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
		return &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: core.Metadata{},
			},
			Volumes: initialVolumes[address],
		}, nil
	}))
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
