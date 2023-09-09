package vm

import (
	"fmt"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

// Assumes the variables have been resolved
func (m *Machine) resolveBalances(getBalance func(core.AccountAddress, core.Asset) (*core.MonetaryInt, error)) error {
	for neededBalance := range m.program.NeededBalances {
		// Account
		account, err := EvalAs[core.AccountAddress](m, neededBalance.Account)
		if err != nil {
			return fmt.Errorf("failed to get required balances: %v", err)
		}

		// Asset
		assetOrAmount, err := m.Eval(neededBalance.AssetOrAmount)
		if err != nil {
			return fmt.Errorf("failed to get required balances: %v", err)
		}
		var asset core.Asset
		switch v := assetOrAmount.(type) {
		case core.Asset:
			asset = v
		case core.Monetary:
			asset = v.Asset
		default:
			return errors.New(coreError)
		}

		// World starts with balance 0
		if string(*account) == "world" {
			m.addBalance(*account, asset, *core.NewMonetaryInt(0))
			continue
		}

		// Fetch balance
		balance, err := getBalance(*account, asset)
		if err != nil {
			return fmt.Errorf("failed to get balance of account %s for asset %s: %s", account, asset, err)
		}
		m.addBalance(*account, asset, *balance)
	}
	return nil
}

func (m *Machine) addBalance(account core.AccountAddress, asset core.Asset, amount core.MonetaryInt) {
	if _, ok := m.balances[account]; !ok {
		m.balances[account] = make(map[core.Asset]*core.MonetaryInt)
	}
	m.balances[account][asset] = &amount
}
