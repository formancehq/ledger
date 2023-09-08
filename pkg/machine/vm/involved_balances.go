package vm

import (
	"fmt"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

// Assumes the variables have been resolved
func (m *Machine) resolveBalances(getBalance func(core.AccountAddress, core.Asset) (*core.MonetaryInt, error)) error {
	for neededBalance := range m.program.NeededBalances {
		assetOrAmount, err := m.Eval(neededBalance.AssetOrAmount)
		if err != nil {
			return fmt.Errorf("failed to get required balances: %v", err)
		}
		account, err := EvalAs[core.AccountAddress](m, neededBalance.Account)
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
		balance, err := getBalance(*account, asset)
		if err != nil {
			return fmt.Errorf("failed to get balance of account %s for asset %s: %s", account, asset, err)
		}
		m.addBalance(*account, asset, balance)
	}
	return nil
}

func (m *Machine) addBalance(account core.AccountAddress, asset core.Asset, amount core.Number) {
	if _, ok := m.balances[account]; !ok {
		m.balances[account] = make(map[core.Asset]core.Number)
	}
	m.balances[account][asset] = amount
}
