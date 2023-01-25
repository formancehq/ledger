package main

import (
	"fmt"

	"github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/compiler"
	"github.com/formancehq/machine/vm"
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

	machine := vm.NewMachine(*program)
	machine.Debug = true

	if err = machine.SetVars(map[string]core.Value{
		"dest": core.Account("charlie"),
	}); err != nil {
		panic(err)
	}

	initialBalances := map[string]map[string]*core.MonetaryInt{
		"alice": {"COIN": core.NewMonetaryInt(10)},
		"bob":   {"COIN": core.NewMonetaryInt(100)},
	}

	{
		ch, err := machine.ResolveResources()
		if err != nil {
			panic(err)
		}
		for req := range ch {
			if req.Error != nil {
				panic(req.Error)
			}
		}
	}

	{
		ch, err := machine.ResolveBalances()
		if err != nil {
			panic(err)
		}
		for req := range ch {
			val := initialBalances[req.Account][req.Asset]
			if req.Error != nil {
				panic(req.Error)
			}
			req.Response <- val
		}
	}

	exitCode, err := machine.Execute()
	if err != nil {
		panic(err)
	}

	fmt.Println("Exit code:", exitCode)
	fmt.Println(machine.Postings)
	fmt.Println(machine.TxMeta)
}
