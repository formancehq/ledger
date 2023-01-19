package ledger

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/davecgh/go-spew/spew"
	machine "github.com/formancehq/machine/core"
	"github.com/formancehq/machine/vm/program"
	"github.com/logrusorgru/aurora"
	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

const (
	EXIT_OK = byte(iota + 1)
	EXIT_FAIL
	EXIT_FAIL_INVALID
	EXIT_FAIL_INSUFFICIENT_FUNDS
)

type Machine struct {
	P                   uint
	Program             program.Program
	Vars                map[string]machine.Value
	UnresolvedResources []program.Resource
	Resources           []machine.Value // Constants and Variables
	resolveCalled       bool
	Balances            map[machine.Account]map[machine.Asset]*machine.MonetaryInt // keeps tracks of balances throughout execution
	setBalanceCalled    bool
	Stack               []machine.Value
	Postings            []Posting                                    // accumulates postings throughout execution
	TxMeta              map[string]machine.Value                     // accumulates transaction meta throughout execution
	AccountsMeta        map[machine.Account]map[string]machine.Value // accumulates accounts meta throughout execution
	Printer             func(chan machine.Value)
	printChan           chan machine.Value
	Debug               bool
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
		Program:             p,
		UnresolvedResources: p.Resources,
		Resources:           make([]machine.Value, 0),
		printChan:           printChan,
		Printer:             StdOutPrinter,
		Postings:            make([]Posting, 0),
		TxMeta:              map[string]machine.Value{},
		AccountsMeta:        map[machine.Account]map[string]machine.Value{},
	}

	return &m
}

func StdOutPrinter(c chan machine.Value) {
	for v := range c {
		fmt.Println("OUT:", v)
	}
}

func (m *Machine) GetTxMetaJSON() Metadata {
	meta := make(Metadata)
	for k, v := range m.TxMeta {
		valJSON, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		v, err := json.Marshal(machine.ValueJSON{
			Type:  v.GetType().String(),
			Value: valJSON,
		})
		if err != nil {
			panic(err)
		}
		meta[k] = v
	}
	return meta
}

func (m *Machine) GetAccountsMetaJSON() Metadata {
	res := Metadata{}
	for account, meta := range m.AccountsMeta {
		for k, v := range meta {
			if _, ok := res[account.String()]; !ok {
				res[account.String()] = map[string][]byte{}
			}
			valJSON, err := json.Marshal(v)
			if err != nil {
				panic(err)
			}
			v, err := json.Marshal(machine.ValueJSON{
				Type:  v.GetType().String(),
				Value: valJSON,
			})
			if err != nil {
				panic(err)
			}
			res[account.String()].(map[string][]byte)[k] = v
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

func (m *Machine) withdrawAll(account machine.Account, asset machine.Asset, overdraft *machine.MonetaryInt) (*machine.Funding, error) {
	if accBalances, ok := m.Balances[account]; ok {
		if balance, ok := accBalances[asset]; ok {
			amountTaken := machine.NewMonetaryInt(0)
			if balance.Add(overdraft).Gt(machine.NewMonetaryInt(0)) {
				amountTaken = balance.Add(overdraft)
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

func (m *Machine) withdrawAlways(account machine.Account, mon machine.Monetary) (*machine.Funding, error) {
	if accBalance, ok := m.Balances[account]; ok {
		if balance, ok := accBalance[mon.Asset]; ok {
			accBalance[mon.Asset] = balance.Sub(mon.Amount)
			return &machine.Funding{
				Asset: mon.Asset,
				Parts: []machine.FundingPart{{
					Account: account,
					Amount:  mon.Amount,
				}},
			}, nil
		}
	}
	return nil, fmt.Errorf("missing %v balance from %v", mon.Asset, account)
}

func (m *Machine) credit(account machine.Account, funding machine.Funding) {
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
		balance := m.Balances[part.Account][funding.Asset]
		m.Balances[part.Account][funding.Asset] = balance.Add(part.Amount)
	}
}

func (m *Machine) tick() (bool, byte) {
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
			return true, EXIT_FAIL
		}
		m.Stack = append(m.Stack, *v)
		m.P += 2

	case program.OP_IPUSH:
		bytes := m.Program.Instructions[m.P+1 : m.P+9]
		v := machine.Number(big.NewInt(int64(binary.LittleEndian.Uint64(bytes))))
		m.Stack = append(m.Stack, v)
		m.P += 8

	case program.OP_BUMP:
		n := big.Int(*pop[machine.Number](m))
		idx := len(m.Stack) - int(n.Uint64()) - 1
		v := m.Stack[idx]
		m.Stack = append(m.Stack[:idx], m.Stack[idx+1:]...)
		m.Stack = append(m.Stack, v)

	case program.OP_DELETE:
		n := m.popValue()
		if n.GetType() == machine.TypeFunding {
			return true, EXIT_FAIL_INVALID
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
		return true, EXIT_FAIL

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
			return true, EXIT_FAIL_INVALID
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
			return true, EXIT_FAIL_INVALID
		}
		m.pushValue(machine.Monetary{
			Asset:  a.Asset,
			Amount: a.Amount.Add(b.Amount),
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
			return true, EXIT_FAIL_INVALID
		}
		m.pushValue(*allotment)

	case program.OP_TAKE_ALL:
		overdraft := pop[machine.Monetary](m)
		account := pop[machine.Account](m)
		funding, err := m.withdrawAll(account, overdraft.Asset, overdraft.Amount)
		if err != nil {
			return true, EXIT_FAIL_INVALID
		}
		m.pushValue(*funding)

	case program.OP_TAKE_ALWAYS:
		mon := pop[machine.Monetary](m)
		account := pop[machine.Account](m)
		funding, err := m.withdrawAlways(account, mon)
		if err != nil {
			return true, EXIT_FAIL_INVALID
		}
		m.pushValue(*funding)

	case program.OP_TAKE:
		mon := pop[machine.Monetary](m)
		funding := pop[machine.Funding](m)
		if funding.Asset != mon.Asset {
			return true, EXIT_FAIL_INVALID
		}
		result, remainder, err := funding.Take(mon.Amount)
		if err != nil {
			return true, EXIT_FAIL_INSUFFICIENT_FUNDS
		}
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_TAKE_MAX:
		mon := pop[machine.Monetary](m)
		funding := pop[machine.Funding](m)
		if funding.Asset != mon.Asset {
			return true, EXIT_FAIL_INVALID
		}
		missing := machine.NewMonetaryInt(0)
		total := funding.Total()
		if mon.Amount.Gt(total) {
			missing = mon.Amount.Sub(total)
		}
		result, remainder := funding.TakeMax(mon.Amount)
		m.pushValue(machine.Monetary{
			Asset:  mon.Asset,
			Amount: missing,
		})
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_FUNDING_ASSEMBLE:
		num := pop[machine.Number](m)
		n := int(num.Uint64())
		if n == 0 {
			return true, EXIT_FAIL_INVALID
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
				return true, EXIT_FAIL_INVALID
			}
			fundings_rev[i] = f
		}
		for i := 0; i < n; i++ {
			res, err := result.Concat(fundings_rev[n-1-i])
			if err != nil {
				return true, EXIT_FAIL_INVALID
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
		dest := pop[machine.Account](m)
		funding := pop[machine.Funding](m)
		m.credit(dest, funding)
		for _, part := range funding.Parts {
			src := part.Account
			amt := part.Amount
			if amt.Eq(machine.NewMonetaryInt(0)) {
				continue
			}
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
		a := pop[machine.Account](m)
		k := pop[machine.String](m)
		v := m.popValue()
		if m.AccountsMeta[a] == nil {
			m.AccountsMeta[a] = map[string]machine.Value{}
		}
		m.AccountsMeta[a][string(k)] = v

	default:
		return true, EXIT_FAIL_INVALID
	}

	m.P += 1

	if int(m.P) >= len(m.Program.Instructions) {
		return true, EXIT_OK
	}

	return false, 0
}

func (m *Machine) Execute() (byte, error) {
	go m.Printer(m.printChan)
	defer close(m.printChan)

	if len(m.Resources) != len(m.UnresolvedResources) {
		return 0, errors.New("resources haven't been initialized")
	} else if m.Balances == nil {
		return 0, errors.New("balances haven't been initialized")
	}

	for {
		finished, exitCode := m.tick()
		if finished {
			if exitCode == EXIT_OK && len(m.Stack) != 0 {
				return EXIT_FAIL_INVALID, nil
			} else {
				return exitCode, nil
			}
		}
	}
}

type BalanceRequest struct {
	Account  string
	Asset    string
	Response chan *machine.MonetaryInt
	Error    error
}

func (m *Machine) ResolveBalances() (chan BalanceRequest, error) {
	if len(m.Resources) != len(m.UnresolvedResources) {
		return nil, errors.New("tried to resolve balances before resources")
	}
	if m.setBalanceCalled {
		return nil, errors.New("tried to call ResolveBalances twice")
	}
	m.setBalanceCalled = true
	resChan := make(chan BalanceRequest)
	go func() {
		defer close(resChan)
		m.Balances = make(map[machine.Account]map[machine.Asset]*machine.MonetaryInt)
		// for every account that we need balances of, check if it's there
		for addr, neededAssets := range m.Program.NeededBalances {
			account, ok := m.getResource(addr)
			if !ok {
				resChan <- BalanceRequest{
					Error: errors.New("invalid program (resolve balances: invalid address of account)"),
				}
				return
			}
			if account, ok := (*account).(machine.Account); ok {
				m.Balances[account] = make(map[machine.Asset]*machine.MonetaryInt)
				// for every asset, send request
				for addr := range neededAssets {
					mon, ok := m.getResource(addr)
					if !ok {
						resChan <- BalanceRequest{
							Error: errors.New("invalid program (resolve balances: invalid address of monetary)"),
						}
						return
					}
					if ha, ok := (*mon).(machine.HasAsset); ok {
						asset := ha.GetAsset()
						if string(account) == "world" {
							m.Balances[account][asset] = machine.NewMonetaryInt(0)
							continue
						}
						respChan := make(chan *machine.MonetaryInt)
						resChan <- BalanceRequest{
							Account:  string(account),
							Asset:    string(asset),
							Response: respChan,
						}
						resp, ok := <-respChan
						close(respChan)
						if !ok {
							resChan <- BalanceRequest{
								Error: errors.New("error on response channel"),
							}
							return
						}
						m.Balances[account][asset] = resp
					} else {
						resChan <- BalanceRequest{
							Error: errors.New("invalid program (resolve balances: not an asset)"),
						}
						return
					}
				}
			} else {
				resChan <- BalanceRequest{
					Error: errors.New("incorrect program (resolve balances: not an account)"),
				}
				return
			}
		}
	}()
	return resChan, nil
}

func (m *Machine) ResolveResources(ctx context.Context, l *Ledger, accs map[string]*core.AccountWithVolumes) error {
	if m.resolveCalled {
		return errors.New("tried to call ResolveResources twice")
	}
	m.resolveCalled = true
	for len(m.Resources) != len(m.UnresolvedResources) {
		idx := len(m.Resources)
		res := m.UnresolvedResources[idx]
		var val machine.Value
		switch res := res.(type) {
		case program.Constant:
			val = res.Inner
		case program.Variable:
			var ok bool
			val, ok = m.Vars[res.Name]
			if !ok {
				return fmt.Errorf("missing variable '%s'", res.Name)
			}
		case program.VariableAccountMetadata:
			sourceAccount, ok := m.getResource(res.Account)
			if !ok {
				return fmt.Errorf(
					"variable '%s': tried to request metadata of an account which has not yet been solved",
					res.Name)
			}
			if (*sourceAccount).GetType() != machine.TypeAccount {
				return fmt.Errorf(
					"variable '%s': tried to request metadata on wrong entity: %v instead of account",
					res.Name, (*sourceAccount).GetType())
			}
			account := (*sourceAccount).(machine.Account)
			spew.Dump("VariableAccountMetadata", accs, res.Key)

			if _, ok := accs[string(account)]; !ok {
				var err error
				accs[string(account)], err = l.GetAccount(ctx, string(account))
				if err != nil {
					return errors.Wrap(err,
						fmt.Sprintf("could not get account %q", string(account)))
				}
			}
			entry, ok := accs[string(account)].Metadata[res.Key]
			if !ok {
				return fmt.Errorf("missing key %v in metadata for account %v", res.Key, string(account))
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("err2")
			}
			value, err := machine.NewValueFromTypedJSON(data)
			if err != nil {
				return fmt.Errorf("err3")
			}

			val = *value

			if val == nil {
				return fmt.Errorf("variable '%s': tried to set nil as resource", res.Name)
			}
			if val.GetType() != res.Typ {
				return fmt.Errorf("variable '%s': wrong type: expected %v, got %v",
					res.Name, res.Typ, val.GetType())
			}
		case program.VariableAccountBalance:
			sourceAccount, ok := m.getResource(res.Account)
			if !ok {
				return fmt.Errorf(
					"variable '%s': tried to request balance of an account which has not yet been solved",
					res.Name)
			}
			if (*sourceAccount).GetType() != machine.TypeAccount {
				return fmt.Errorf(
					"variable '%s': tried to request balance on wrong entity: %v instead of account",
					res.Name, (*sourceAccount).GetType())
			}
			account := (*sourceAccount).(machine.Account)

			var amount machine.Value

			if _, ok := accs[string(account)]; !ok {
				var err error
				accs[string(account)], err = l.GetAccount(ctx, string(account))
				if err != nil {
					return errors.Wrap(err,
						fmt.Sprintf("could not get account %q", string(account)))
				}
			}
			amt2 := accs[string(account)].Balances[res.Asset].OrZero()
			r := machine.MonetaryInt(*amt2)

			amount = &r
			if amount.GetType() != machine.TypeNumber {
				return fmt.Errorf(
					"variable '%s': tried to request balance: wrong type received: expected %v, got %v",
					res.Name, machine.TypeNumber, amount.GetType())
			}
			amt := amount.(machine.Number)
			if amt.Ltz() {
				return fmt.Errorf(
					"variable '%s': tried to request the balance of account %s for asset %s: received %s: monetary amounts must be non-negative",
					res.Name, account, res.Asset, amt)
			}
			val = machine.Monetary{
				Asset:  machine.Asset(res.Asset),
				Amount: amt,
			}
		default:
			panic(fmt.Errorf("type %T not implemented", res))
		}
		m.Resources = append(m.Resources, val)
	}

	return nil
}

func (m *Machine) SetVars(vars map[string]machine.Value) error {
	v, err := m.Program.ParseVariables(vars)
	if err != nil {
		return err
	}
	m.Vars = v
	return nil
}

func (m *Machine) SetVarsFromJSON(vars map[string]json.RawMessage) error {
	v, err := m.Program.ParseVariablesJSON(vars)
	if err != nil {
		return err
	}
	m.Vars = v
	return nil
}

func (m *Machine) popValue() machine.Value {
	l := len(m.Stack)
	x := m.Stack[l-1]
	m.Stack = m.Stack[:l-1]
	return x
}

func pop[T machine.Value](m *Machine) T {
	x := m.popValue()
	if v, ok := x.(T); ok {
		return v
	}
	panic(fmt.Errorf("unexpected type '%T' on stack", x))
}

func (m *Machine) pushValue(v machine.Value) {
	m.Stack = append(m.Stack, v)
}
