/*
Provides `Machine`, which executes programs and outputs postings.
1: Create New Machine
2: Set Variables (with `internal.Value`s or JSON)
3: Resolve Resources (answer requests on channel)
4: Resolve Balances (answer requests on channel)
6: Execute
*/
package vm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine"

	"errors"
	"github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/logrusorgru/aurora"
)

type Machine struct {
	P                          uint
	Program                    program.Program
	Vars                       map[string]machine.Value
	UnresolvedResources        []program.Resource
	Resources                  []machine.Value // Constants and Variables
	UnresolvedResourceBalances map[string]int
	resolveCalled              bool
	Balances                   map[machine.AccountAddress]map[machine.Asset]*machine.MonetaryInt // keeps track of balances throughout execution
	Stack                      []machine.Value
	Postings                   []Posting                                           // accumulates postings throughout execution
	TxMeta                     map[string]machine.Value                            // accumulates transaction meta throughout execution
	AccountsMeta               map[machine.AccountAddress]map[string]machine.Value // accumulates accounts meta throughout execution
	Printer                    func(chan machine.Value)
	printChan                  chan machine.Value
	Debug                      bool
}

type Posting struct {
	Source      string               `json:"source"`
	Destination string               `json:"destination"`
	Amount      *machine.MonetaryInt `json:"amount"`
	Asset       string               `json:"asset"`
}

type Metadata map[string]any

func NewMachine(p program.Program) *Machine {
	printChan := make(chan machine.Value)

	m := Machine{
		Program:                    p,
		UnresolvedResources:        p.Resources,
		Resources:                  make([]machine.Value, 0),
		printChan:                  printChan,
		Printer:                    StdOutPrinter,
		Postings:                   make([]Posting, 0),
		TxMeta:                     map[string]machine.Value{},
		AccountsMeta:               map[machine.AccountAddress]map[string]machine.Value{},
		UnresolvedResourceBalances: map[string]int{},
	}

	return &m
}

func StdOutPrinter(c chan machine.Value) {
	for v := range c {
		fmt.Println("OUT:", v)
	}
}

func (m *Machine) GetTxMetaJSON() metadata.Metadata {
	meta := metadata.Metadata{}
	for k, v := range m.TxMeta {
		var err error
		meta[k], err = machine.NewStringFromValue(v)
		if err != nil {
			panic(err)
		}
	}
	return meta
}

func (m *Machine) GetAccountsMetaJSON() map[string]metadata.Metadata {
	res := make(map[string]metadata.Metadata)
	for account, meta := range m.AccountsMeta {
		for k, v := range meta {
			if _, ok := res[string(account)]; !ok {
				res[string(account)] = metadata.Metadata{}
			}

			var err error
			res[string(account)][k], err = machine.NewStringFromValue(v)
			if err != nil {
				panic(err)
			}
		}
	}

	return res
}

func (m *Machine) getResource(addr machine.Address) (*machine.Value, bool) {
	a := int(addr)
	if a >= len(m.Resources) {
		return nil, false
	}
	return &m.Resources[a], true
}

func (m *Machine) withdrawAll(account machine.AccountAddress, asset machine.Asset, overdraft *machine.MonetaryInt) (*machine.Funding, error) {
	if accBalances, ok := m.Balances[account]; ok {
		if balance, ok := accBalances[asset]; ok {
			amountTaken := machine.Zero
			balanceWithOverdraft := balance.Add(overdraft)
			if balanceWithOverdraft.Gt(machine.Zero) {
				amountTaken = balanceWithOverdraft
				accBalances[asset] = overdraft.Neg()
			}

			return &machine.Funding{
				Asset: asset,
				Parts: []machine.FundingPart{{
					Account: account,
					Amount:  amountTaken,
				}},
			}, nil
		}
	}
	return nil, fmt.Errorf("missing %v balance from %v", asset, account)
}

func (m *Machine) withdrawAlways(account machine.AccountAddress, mon machine.Monetary) (*machine.Funding, error) {
	if accBalance, ok := m.Balances[account]; ok {
		if balance, ok := accBalance[mon.Asset]; ok {
			accBalance[mon.Asset] = balance.Sub(mon.Amount)
		}
	}

	return &machine.Funding{
		Asset: mon.Asset,
		Parts: []machine.FundingPart{{
			Account: account,
			Amount:  mon.Amount,
		}},
	}, nil
}

func (m *Machine) credit(account machine.AccountAddress, funding machine.Funding) {
	if account == "world" {
		return
	}
	if accBalance, ok := m.Balances[account]; ok {
		if _, ok := accBalance[funding.Asset]; ok {
			for _, part := range funding.Parts {
				balance := accBalance[funding.Asset]
				accBalance[funding.Asset] = balance.Add(part.Amount)
			}
		}
	}
}

func (m *Machine) repay(funding machine.Funding) {
	for _, part := range funding.Parts {
		if part.Account == "world" {
			continue
		}
		accountBalance, ok := m.Balances[part.Account]
		if !ok {
			// no asset: the source has to be an unbounded source
			// which NEVER appears as bounded
			// this means we don't need to track it's balance
			continue
		}

		balance := accountBalance[funding.Asset]
		accountBalance[funding.Asset] = balance.Add(part.Amount)
	}
}

func (m *Machine) tick() (bool, error) {
	op := m.Program.Instructions[m.P]

	if m.Debug {
		fmt.Println("STATE ---------------------------------------------------------------------")
		fmt.Printf("    %v\n", aurora.Blue(m.Stack))
		fmt.Printf("    %v\n", aurora.Cyan(m.Balances))
		fmt.Printf("    %v\n", program.OpcodeName(op))
	}

	switch op {
	case program.OP_APUSH:
		bytes := m.Program.Instructions[m.P+1 : m.P+3]
		v, ok := m.getResource(machine.Address(binary.LittleEndian.Uint16(bytes)))
		if !ok {
			return true, machine.ErrResourceNotFound
		}
		m.Stack = append(m.Stack, *v)
		m.P += 2

	case program.OP_BUMP:
		n := big.Int(*pop[machine.Number](m))
		idx := len(m.Stack) - int(n.Uint64()) - 1
		v := m.Stack[idx]
		m.Stack = append(m.Stack[:idx], m.Stack[idx+1:]...)
		m.Stack = append(m.Stack, v)

	case program.OP_DELETE:
		n := m.popValue()
		if n.GetType() == machine.TypeFunding {
			return true, machine.NewErrInvalidScript("wrong type: want: %v, got: %v", n.GetType(), machine.TypeFunding)
		}

	case program.OP_IADD:
		b := pop[machine.Number](m)
		a := pop[machine.Number](m)
		m.pushValue(a.Add(b))

	case program.OP_ISUB:
		b := pop[machine.Number](m)
		a := pop[machine.Number](m)
		m.pushValue(a.Sub(b))

	case program.OP_PRINT:
		a := m.popValue()
		m.printChan <- a

	case program.OP_FAIL:
		return true, machine.ErrScriptFailed

	case program.OP_ASSET:
		v := m.popValue()
		switch v := v.(type) {
		case machine.Asset:
			m.pushValue(v)
		case machine.Monetary:
			m.pushValue(v.Asset)
		case machine.Funding:
			m.pushValue(v.Asset)
		default:
			return true, machine.NewErrInvalidScript("wrong type for op asset: %v", v.GetType())
		}

	case program.OP_MONETARY_NEW:
		amount := pop[machine.Number](m)
		asset := pop[machine.Asset](m)
		m.pushValue(machine.Monetary{
			Asset:  asset,
			Amount: amount,
		})

	case program.OP_MONETARY_ADD:
		b := pop[machine.Monetary](m)
		a := pop[machine.Monetary](m)
		if a.Asset != b.Asset {
			return true, machine.NewErrInvalidScript("cannot add different assets: %v and %v", a.Asset, b.Asset)
		}
		m.pushValue(machine.Monetary{
			Asset:  a.Asset,
			Amount: a.Amount.Add(b.Amount),
		})

	case program.OP_MONETARY_SUB:
		b := pop[machine.Monetary](m)
		a := pop[machine.Monetary](m)
		if a.Asset != b.Asset {
			return true, fmt.Errorf("%s", program.OpcodeName(op))
		}
		m.pushValue(machine.Monetary{
			Asset:  a.Asset,
			Amount: a.Amount.Sub(b.Amount),
		})

	case program.OP_MAKE_ALLOTMENT:
		n := pop[machine.Number](m)
		portions := make([]machine.Portion, n.Uint64())
		for i := uint64(0); i < n.Uint64(); i++ {
			p := pop[machine.Portion](m)
			portions[i] = p
		}
		allotment, err := machine.NewAllotment(portions)
		if err != nil {
			return true, machine.NewErrInvalidScript("%s", err)
		}
		m.pushValue(*allotment)

	case program.OP_TAKE_ALL:
		overdraft := pop[machine.Monetary](m)
		account := pop[machine.AccountAddress](m)
		funding, err := m.withdrawAll(account, overdraft.Asset, overdraft.Amount)
		if err != nil {
			return true, machine.NewErrInvalidScript("%s", err)
		}
		m.pushValue(*funding)

	case program.OP_TAKE_ALWAYS:
		mon := pop[machine.Monetary](m)
		account := pop[machine.AccountAddress](m)
		funding, err := m.withdrawAlways(account, mon)
		if err != nil {
			return true, machine.NewErrInvalidScript("%s", err)
		}
		m.pushValue(*funding)

	case program.OP_TAKE:
		mon := pop[machine.Monetary](m)
		funding := pop[machine.Funding](m)
		if funding.Asset != mon.Asset {
			return true, machine.NewErrInvalidScript("cannot take from different assets: %v and %v", funding.Asset, mon.Asset)
		}
		result, remainder, err := funding.Take(mon.Amount)
		if err != nil {
			return true, machine.NewErrInsufficientFund("%s", err)
		}
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_TAKE_MAX:
		mon := pop[machine.Monetary](m)
		if mon.Amount.Ltz() {
			return true, fmt.Errorf(
				"cannot send a monetary with a negative amount: [%s %s]",
				string(mon.Asset), mon.Amount)
		}
		funding := pop[machine.Funding](m)
		if funding.Asset != mon.Asset {
			return true, machine.NewErrInvalidScript("cannot take from different assets: %v and %v", funding.Asset, mon.Asset)
		}
		missing := machine.Zero
		total := funding.Total()
		if mon.Amount.Gt(total) {
			missing = mon.Amount.Sub(total)
		}
		m.pushValue(machine.Monetary{
			Asset:  mon.Asset,
			Amount: missing,
		})
		result, remainder := funding.TakeMax(mon.Amount)
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_FUNDING_ASSEMBLE:
		num := pop[machine.Number](m)
		n := int(num.Uint64())
		if n == 0 {
			return true, machine.NewErrInvalidScript("cannot assemble zero fundings")
		}
		first := pop[machine.Funding](m)
		result := machine.Funding{
			Asset: first.Asset,
		}
		fundings_rev := make([]machine.Funding, n)
		fundings_rev[0] = first
		for i := 1; i < n; i++ {
			f := pop[machine.Funding](m)
			if f.Asset != result.Asset {
				return true, machine.NewErrInvalidScript("cannot assemble different assets: %v and %v", f.Asset, result.Asset)
			}
			fundings_rev[i] = f
		}
		for i := 0; i < n; i++ {
			res, err := result.Concat(fundings_rev[n-1-i])
			if err != nil {
				return true, machine.NewErrInvalidScript("%s", err)
			}
			result = res
		}
		m.pushValue(result)

	case program.OP_FUNDING_SUM:
		funding := pop[machine.Funding](m)
		sum := funding.Total()
		m.pushValue(funding)
		m.pushValue(machine.Monetary{
			Asset:  funding.Asset,
			Amount: sum,
		})

	case program.OP_FUNDING_REVERSE:
		funding := pop[machine.Funding](m)
		result := funding.Reverse()
		m.pushValue(result)

	case program.OP_ALLOC:
		allotment := pop[machine.Allotment](m)
		monetary := pop[machine.Monetary](m)
		total := monetary.Amount
		parts := allotment.Allocate(total)
		for i := len(parts) - 1; i >= 0; i-- {
			m.pushValue(machine.Monetary{
				Asset:  monetary.Asset,
				Amount: parts[i],
			})
		}

	case program.OP_REPAY:
		m.repay(pop[machine.Funding](m))

	case program.OP_SEND:
		dest := pop[machine.AccountAddress](m)
		funding := pop[machine.Funding](m)
		m.credit(dest, funding)
		for _, part := range funding.Parts {
			src := part.Account
			amt := part.Amount
			m.Postings = append(m.Postings, Posting{
				Source:      string(src),
				Destination: string(dest),
				Asset:       string(funding.Asset),
				Amount:      amt,
			})
		}

	case program.OP_TX_META:
		k := pop[machine.String](m)
		v := m.popValue()
		m.TxMeta[string(k)] = v

	case program.OP_ACCOUNT_META:
		a := pop[machine.AccountAddress](m)
		k := pop[machine.String](m)
		v := m.popValue()
		if m.AccountsMeta[a] == nil {
			m.AccountsMeta[a] = map[string]machine.Value{}
		}
		m.AccountsMeta[a][string(k)] = v

	case program.OP_SAVE:
		a := pop[machine.AccountAddress](m)
		v := m.popValue()
		switch v := v.(type) {
		case machine.Asset:
			m.Balances[a][v] = machine.Zero
		case machine.Monetary:
			m.Balances[a][v.Asset] = m.Balances[a][v.Asset].Sub(v.Amount)
		default:
			panic(fmt.Errorf("invalid value type: %T", v))
		}

	default:
		return true, machine.NewErrInvalidScript("invalid opcode: %v", op)
	}

	m.P += 1

	if int(m.P) >= len(m.Program.Instructions) {
		return true, nil
	}

	return false, nil
}

func (m *Machine) Execute() error {
	go m.Printer(m.printChan)
	defer close(m.printChan)

	if len(m.Resources) != len(m.UnresolvedResources) {
		return machine.ErrResourcesNotInitialized
	} else if m.Balances == nil {
		return machine.ErrBalancesNotInitialized
	}

	for {
		finished, err := m.tick()
		if finished {
			if err == nil && len(m.Stack) != 0 {
				panic("stack not empty after execution")
			} else {
				return err
			}
		}
	}
}

func (m *Machine) ResolveBalances(ctx context.Context, store Store) error {

	// map account/asset/resourceIndex
	assignBalanceAsResource := map[string]map[string]int{}

	balancesQuery := BalanceQuery{}
	for address, resourceIndex := range m.UnresolvedResourceBalances {
		monetary := m.Resources[resourceIndex].(machine.Monetary)
		balancesQuery[address] = append(balancesQuery[address], string(monetary.Asset))

		if _, ok := assignBalanceAsResource[address]; !ok {
			assignBalanceAsResource[address] = map[string]int{}
		}
		assignBalanceAsResource[address][string(monetary.Asset)] = resourceIndex
	}

	// for every account that we need balances of, check if it's there
	for addr, neededAssets := range m.Program.NeededBalances {
		account, ok := m.getResource(addr)
		if !ok {
			return errors.New("invalid program (resolve balances: invalid address of account)")
		}
		accountAddress := (*account).(machine.AccountAddress)

		// for every asset, register the query
		for addr := range neededAssets {
			mon, ok := m.getResource(addr)
			if !ok {
				return errors.New("invalid program (resolve balances: invalid address of monetary)")
			}

			asset := (*mon).(machine.HasAsset).GetAsset()
			if string(accountAddress) == "world" {
				m.Balances[accountAddress][asset] = machine.Zero
				continue
			}

			balancesQuery[string(accountAddress)] = append(balancesQuery[string(accountAddress)], string(asset))
		}
	}

	m.Balances = make(map[machine.AccountAddress]map[machine.Asset]*machine.MonetaryInt)
	if len(balancesQuery) > 0 {
		balances, err := store.GetBalances(ctx, balancesQuery)
		if err != nil {
			return fmt.Errorf("could not get balances: %w", err)
		}

		for account, forAssets := range balances {
			for asset, balance := range forAssets {
				if assignBalanceAsResource[account] != nil {
					resourceIndex, ok := assignBalanceAsResource[account][asset]
					if ok {
						if balance.Cmp(ledger.Zero) < 0 {
							return machine.NewErrNegativeAmount("tried to request the balance of account %s for asset %s: received %s: monetary amounts must be non-negative",
								account, asset, balance)
						}
						monetary := m.Resources[resourceIndex].(machine.Monetary)
						monetary.Amount = machine.NewMonetaryIntFromBigInt(balance)
						m.Resources[resourceIndex] = monetary
					}
				}

				if _, ok := m.Balances[machine.AccountAddress(account)]; !ok {
					m.Balances[machine.AccountAddress(account)] = make(map[machine.Asset]*machine.MonetaryInt)
				}
				m.Balances[machine.AccountAddress(account)][machine.Asset(asset)] = machine.NewMonetaryIntFromBigInt(balance)
			}
		}
	}

	return nil
}

func (m *Machine) ResolveResources(ctx context.Context, store Store) error {
	if m.resolveCalled {
		return errors.New("tried to call ResolveResources twice")
	}

	m.resolveCalled = true
	involvedAccountsMap := make(map[machine.Address]string)
	for len(m.Resources) != len(m.UnresolvedResources) {
		idx := len(m.Resources)
		res := m.UnresolvedResources[idx]
		var val machine.Value
		switch res := res.(type) {
		case program.Constant:
			val = res.Inner
			if val.GetType() == machine.TypeAccount {
				involvedAccountsMap[machine.Address(idx)] = string(val.(machine.AccountAddress))
			}
		case program.Variable:
			var ok bool
			val, ok = m.Vars[res.Name]
			if !ok {
				return fmt.Errorf("missing variable '%s'", res.Name)
			}
			if val.GetType() == machine.TypeAccount {
				involvedAccountsMap[machine.Address(idx)] = string(val.(machine.AccountAddress))
			}
		case program.VariableAccountMetadata:
			acc, _ := m.getResource(res.Account)
			addr := string((*acc).(machine.AccountAddress))

			account, err := store.GetAccount(ctx, addr)
			if err != nil {
				return err
			}

			metadata, ok := account.Metadata[res.Key]
			if !ok {
				return machine.NewErrMissingMetadata("missing key %v in metadata for account %s", res.Key, addr)
			}

			val, err = machine.NewValueFromString(res.Typ, metadata)
			if err != nil {
				return err
			}
			if val.GetType() == machine.TypeAccount {
				involvedAccountsMap[machine.Address(idx)] = string(val.(machine.AccountAddress))
			}
		case program.VariableAccountBalance:
			acc, _ := m.getResource(res.Account)
			address := string((*acc).(machine.AccountAddress))
			involvedAccountsMap[machine.Address(idx)] = address
			m.UnresolvedResourceBalances[address] = idx

			ass, ok := m.getResource(res.Asset)
			if !ok {
				return fmt.Errorf(
					"variable '%s': tried to request account balance of an asset which has not yet been solved",
					res.Name)
			}
			if (*ass).GetType() != machine.TypeAsset {
				return fmt.Errorf(
					"variable '%s': tried to request account balance for an asset on wrong entity: %v instead of asset",
					res.Name, (*ass).GetType())
			}

			val = machine.Monetary{
				Asset: (*ass).(machine.Asset),
			}
		case program.Monetary:
			ass, _ := m.getResource(res.Asset)
			val = machine.Monetary{
				Asset:  (*ass).(machine.Asset),
				Amount: res.Amount,
			}
		default:
			panic(fmt.Errorf("type %T not implemented", res))
		}
		m.Resources = append(m.Resources, val)
	}

	return nil
}

func (m *Machine) SetVarsFromJSON(vars map[string]string) error {
	v, err := m.Program.ParseVariablesJSON(vars)
	if err != nil {
		return machine.NewErrInvalidVars("%s", err)
	}
	m.Vars = v
	return nil
}
