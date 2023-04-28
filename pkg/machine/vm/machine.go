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
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/internal"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/errors"
)

type Machine struct {
	P                          uint
	Program                    program.Program
	Vars                       map[string]internal.Value
	UnresolvedResources        []program.Resource
	Resources                  []internal.Value // Constants and Variables
	UnresolvedResourceBalances map[string]int
	resolveCalled              bool
	Balances                   map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt // keeps track of balances throughout execution
	setBalanceCalled           bool
	Stack                      []internal.Value
	Postings                   []Posting                                             // accumulates postings throughout execution
	TxMeta                     map[string]internal.Value                             // accumulates transaction meta throughout execution
	AccountsMeta               map[internal.AccountAddress]map[string]internal.Value // accumulates accounts meta throughout execution
	Printer                    func(chan internal.Value)
	printChan                  chan internal.Value
	Debug                      bool
}

type Posting struct {
	Source      string                `json:"source"`
	Destination string                `json:"destination"`
	Amount      *internal.MonetaryInt `json:"amount"`
	Asset       string                `json:"asset"`
}

type Metadata map[string]any

func NewMachine(p program.Program) *Machine {
	printChan := make(chan internal.Value)

	m := Machine{
		Program:                    p,
		UnresolvedResources:        p.Resources,
		Resources:                  make([]internal.Value, 0),
		printChan:                  printChan,
		Printer:                    StdOutPrinter,
		Postings:                   make([]Posting, 0),
		TxMeta:                     map[string]internal.Value{},
		AccountsMeta:               map[internal.AccountAddress]map[string]internal.Value{},
		UnresolvedResourceBalances: map[string]int{},
	}

	return &m
}

func StdOutPrinter(c chan internal.Value) {
	for v := range c {
		fmt.Println("OUT:", v)
	}
}

func (m *Machine) GetTxMetaJSON() metadata.Metadata {
	meta := metadata.Metadata{}
	for k, v := range m.TxMeta {
		valJSON, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		v, err := json.Marshal(internal.ValueJSON{
			Type:  v.GetType().String(),
			Value: valJSON,
		})
		if err != nil {
			panic(err)
		}
		meta[k] = string(v)
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
			valJSON, err := json.Marshal(v)
			if err != nil {
				panic(err)
			}
			v, err := json.Marshal(internal.ValueJSON{
				Type:  v.GetType().String(),
				Value: valJSON,
			})
			if err != nil {
				panic(err)
			}
			res[string(account)][k] = string(v)
		}
	}

	return res
}

func (m *Machine) getResource(addr internal.Address) (*internal.Value, bool) {
	a := int(addr)
	if a >= len(m.Resources) {
		return nil, false
	}
	return &m.Resources[a], true
}

func (m *Machine) withdrawAll(account internal.AccountAddress, asset internal.Asset, overdraft *internal.MonetaryInt) (*internal.Funding, error) {
	if accBalances, ok := m.Balances[account]; ok {
		if balance, ok := accBalances[asset]; ok {
			amountTaken := internal.Zero
			balanceWithOverdraft := balance.Add(overdraft)
			if balanceWithOverdraft.Gt(internal.Zero) {
				amountTaken = balanceWithOverdraft
				accBalances[asset] = overdraft.Neg()
			}

			return &internal.Funding{
				Asset: asset,
				Parts: []internal.FundingPart{{
					Account: account,
					Amount:  amountTaken,
				}},
			}, nil
		}
	}
	return nil, fmt.Errorf("missing %v balance from %v", asset, account)
}

func (m *Machine) withdrawAlways(account internal.AccountAddress, mon internal.Monetary) (*internal.Funding, error) {
	if accBalance, ok := m.Balances[account]; ok {
		if balance, ok := accBalance[mon.Asset]; ok {
			accBalance[mon.Asset] = balance.Sub(mon.Amount)
			return &internal.Funding{
				Asset: mon.Asset,
				Parts: []internal.FundingPart{{
					Account: account,
					Amount:  mon.Amount,
				}},
			}, nil
		}
	}
	return nil, fmt.Errorf("missing %v balance from %v", mon.Asset, account)
}

func (m *Machine) credit(account internal.AccountAddress, funding internal.Funding) {
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

func (m *Machine) repay(funding internal.Funding) {
	for _, part := range funding.Parts {
		if part.Account == "world" {
			continue
		}
		balance := m.Balances[part.Account][funding.Asset]
		m.Balances[part.Account][funding.Asset] = balance.Add(part.Amount)
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
		v, ok := m.getResource(internal.Address(binary.LittleEndian.Uint16(bytes)))
		if !ok {
			return true, ErrResourceNotFound
		}
		m.Stack = append(m.Stack, *v)
		m.P += 2

	case program.OP_BUMP:
		n := big.Int(*pop[internal.Number](m))
		idx := len(m.Stack) - int(n.Uint64()) - 1
		v := m.Stack[idx]
		m.Stack = append(m.Stack[:idx], m.Stack[idx+1:]...)
		m.Stack = append(m.Stack, v)

	case program.OP_DELETE:
		n := m.popValue()
		if n.GetType() == internal.TypeFunding {
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.Errorf("wrong type: want: %v, got: %v", n.GetType(), internal.TypeFunding))
		}

	case program.OP_IADD:
		b := pop[internal.Number](m)
		a := pop[internal.Number](m)
		m.pushValue(a.Add(b))

	case program.OP_ISUB:
		b := pop[internal.Number](m)
		a := pop[internal.Number](m)
		m.pushValue(a.Sub(b))

	case program.OP_PRINT:
		a := m.popValue()
		m.printChan <- a

	case program.OP_FAIL:
		return true, ErrScriptFailed

	case program.OP_ASSET:
		v := m.popValue()
		switch v := v.(type) {
		case internal.Asset:
			m.pushValue(v)
		case internal.Monetary:
			m.pushValue(v.Asset)
		case internal.Funding:
			m.pushValue(v.Asset)
		default:
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.Errorf("wrong type for op asset: %v", v.GetType()))
		}

	case program.OP_MONETARY_NEW:
		amount := pop[internal.Number](m)
		asset := pop[internal.Asset](m)
		m.pushValue(internal.Monetary{
			Asset:  asset,
			Amount: amount,
		})

	case program.OP_MONETARY_ADD:
		b := pop[internal.Monetary](m)
		a := pop[internal.Monetary](m)
		if a.Asset != b.Asset {
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.Errorf("cannot add different assets: %v and %v", a.Asset, b.Asset))
		}
		m.pushValue(internal.Monetary{
			Asset:  a.Asset,
			Amount: a.Amount.Add(b.Amount),
		})

	case program.OP_MONETARY_SUB:
		b := pop[internal.Monetary](m)
		a := pop[internal.Monetary](m)
		if a.Asset != b.Asset {
			return true, fmt.Errorf("%s", program.OpcodeName(op))
		}
		m.pushValue(internal.Monetary{
			Asset:  a.Asset,
			Amount: a.Amount.Sub(b.Amount),
		})

	case program.OP_MAKE_ALLOTMENT:
		n := pop[internal.Number](m)
		portions := make([]internal.Portion, n.Uint64())
		for i := uint64(0); i < n.Uint64(); i++ {
			p := pop[internal.Portion](m)
			portions[i] = p
		}
		allotment, err := internal.NewAllotment(portions)
		if err != nil {
			return true, errorsutil.NewError(ErrInvalidScript, err)
		}
		m.pushValue(*allotment)

	case program.OP_TAKE_ALL:
		overdraft := pop[internal.Monetary](m)
		account := pop[internal.AccountAddress](m)
		funding, err := m.withdrawAll(account, overdraft.Asset, overdraft.Amount)
		if err != nil {
			return true, errorsutil.NewError(ErrInvalidScript, err)
		}
		m.pushValue(*funding)

	case program.OP_TAKE_ALWAYS:
		mon := pop[internal.Monetary](m)
		account := pop[internal.AccountAddress](m)
		funding, err := m.withdrawAlways(account, mon)
		if err != nil {
			return true, errorsutil.NewError(ErrInvalidScript, err)
		}
		m.pushValue(*funding)

	case program.OP_TAKE:
		mon := pop[internal.Monetary](m)
		funding := pop[internal.Funding](m)
		if funding.Asset != mon.Asset {
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.Errorf("cannot take from different assets: %v and %v", funding.Asset, mon.Asset))
		}
		result, remainder, err := funding.Take(mon.Amount)
		if err != nil {
			return true, errorsutil.NewError(ErrInsufficientFund, err)
		}
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_TAKE_MAX:
		mon := pop[internal.Monetary](m)
		if mon.Amount.Ltz() {
			return true, fmt.Errorf(
				"cannot send a monetary with a negative amount: [%s %s]",
				string(mon.Asset), mon.Amount)
		}
		funding := pop[internal.Funding](m)
		if funding.Asset != mon.Asset {
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.Errorf("cannot take from different assets: %v and %v", funding.Asset, mon.Asset))
		}
		missing := internal.Zero
		total := funding.Total()
		if mon.Amount.Gt(total) {
			missing = mon.Amount.Sub(total)
		}
		m.pushValue(internal.Monetary{
			Asset:  mon.Asset,
			Amount: missing,
		})
		result, remainder := funding.TakeMax(mon.Amount)
		m.pushValue(remainder)
		m.pushValue(result)

	case program.OP_FUNDING_ASSEMBLE:
		num := pop[internal.Number](m)
		n := int(num.Uint64())
		if n == 0 {
			return true, errorsutil.NewError(ErrInvalidScript,
				errors.New("cannot assemble zero fundings"))
		}
		first := pop[internal.Funding](m)
		result := internal.Funding{
			Asset: first.Asset,
		}
		fundings_rev := make([]internal.Funding, n)
		fundings_rev[0] = first
		for i := 1; i < n; i++ {
			f := pop[internal.Funding](m)
			if f.Asset != result.Asset {
				return true, errorsutil.NewError(ErrInvalidScript,
					errors.Errorf("cannot assemble different assets: %v and %v", f.Asset, result.Asset))
			}
			fundings_rev[i] = f
		}
		for i := 0; i < n; i++ {
			res, err := result.Concat(fundings_rev[n-1-i])
			if err != nil {
				return true, errorsutil.NewError(ErrInvalidScript, err)
			}
			result = res
		}
		m.pushValue(result)

	case program.OP_FUNDING_SUM:
		funding := pop[internal.Funding](m)
		sum := funding.Total()
		m.pushValue(funding)
		m.pushValue(internal.Monetary{
			Asset:  funding.Asset,
			Amount: sum,
		})

	case program.OP_FUNDING_REVERSE:
		funding := pop[internal.Funding](m)
		result := funding.Reverse()
		m.pushValue(result)

	case program.OP_ALLOC:
		allotment := pop[internal.Allotment](m)
		monetary := pop[internal.Monetary](m)
		total := monetary.Amount
		parts := allotment.Allocate(total)
		for i := len(parts) - 1; i >= 0; i-- {
			m.pushValue(internal.Monetary{
				Asset:  monetary.Asset,
				Amount: parts[i],
			})
		}

	case program.OP_REPAY:
		m.repay(pop[internal.Funding](m))

	case program.OP_SEND:
		dest := pop[internal.AccountAddress](m)
		funding := pop[internal.Funding](m)
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
		k := pop[internal.String](m)
		v := m.popValue()
		m.TxMeta[string(k)] = v

	case program.OP_ACCOUNT_META:
		a := pop[internal.AccountAddress](m)
		k := pop[internal.String](m)
		v := m.popValue()
		if m.AccountsMeta[a] == nil {
			m.AccountsMeta[a] = map[string]internal.Value{}
		}
		m.AccountsMeta[a][string(k)] = v

	case program.OP_SAVE:
		a := pop[internal.AccountAddress](m)
		v := m.popValue()
		switch v := v.(type) {
		case internal.Asset:
			m.Balances[a][v] = internal.Zero
		case internal.Monetary:
			m.Balances[a][v.Asset] = m.Balances[a][v.Asset].Sub(v.Amount)
		default:
			panic(fmt.Errorf("invalid value type: %T", v))
		}

	default:
		return true, errorsutil.NewError(ErrInvalidScript,
			errors.Errorf("invalid opcode: %v", op))
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
		return ErrResourcesNotInitialized
	} else if m.Balances == nil {
		return ErrBalancesNotInitialized
	}

	for {
		finished, err := m.tick()
		if finished {
			if err == nil && len(m.Stack) != 0 {
				return errorsutil.NewError(ErrInvalidScript,
					errors.New("stack not empty after execution"))
			} else {
				return err
			}
		}
	}
}

type BalanceRequest struct {
	Account  string
	Asset    string
	Response chan *internal.MonetaryInt
	Error    error
}

func (m *Machine) ResolveBalances(ctx context.Context, store Store) error {
	if len(m.Resources) != len(m.UnresolvedResources) {
		return errors.New("tried to resolve balances before resources")
	}
	if m.setBalanceCalled {
		return errors.New("tried to call ResolveBalances twice")
	}
	m.setBalanceCalled = true
	m.Balances = make(map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt)

	for address, resourceIndex := range m.UnresolvedResourceBalances {
		monetary := m.Resources[resourceIndex].(internal.Monetary)
		balance, err := store.GetBalanceFromLogs(ctx, address, string(monetary.Asset))
		if err != nil {
			return err
		}
		if balance.Cmp(core.Zero) < 0 {
			return errorsutil.NewError(ErrNegativeMonetaryAmount, fmt.Errorf(
				"tried to request the balance of account %s for asset %s: received %s: monetary amounts must be non-negative",
				address, monetary.Asset, balance))
		}
		monetary.Amount = internal.NewMonetaryIntFromBigInt(balance)
		m.Resources[resourceIndex] = monetary
	}

	// for every account that we need balances of, check if it's there
	for addr, neededAssets := range m.Program.NeededBalances {
		account, ok := m.getResource(addr)
		if !ok {
			return errors.New("invalid program (resolve balances: invalid address of account)")
		}
		accountAddress := (*account).(internal.AccountAddress)
		m.Balances[accountAddress] = make(map[internal.Asset]*internal.MonetaryInt)
		// for every asset, send request
		for addr := range neededAssets {
			mon, ok := m.getResource(addr)
			if !ok {
				return errors.New("invalid program (resolve balances: invalid address of monetary)")
			}

			asset := (*mon).(internal.HasAsset).GetAsset()
			if string(accountAddress) == "world" {
				m.Balances[accountAddress][asset] = internal.Zero
				continue
			}

			balance, err := store.GetBalanceFromLogs(ctx, string(accountAddress), string(asset))
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("could not get balance for account %q", addr))
			}

			m.Balances[accountAddress][asset] = internal.NewMonetaryIntFromBigInt(balance)
		}
	}
	return nil
}

func (m *Machine) ResolveResources(ctx context.Context, store Store) ([]string, []string, error) {
	//TODO(gfyrag): Is that really required? Feel like defensive programming.
	if m.resolveCalled {
		return nil, nil, errors.New("tried to call ResolveResources twice")
	}

	m.resolveCalled = true
	involvedAccountsMap := make(map[internal.Address]string)
	for len(m.Resources) != len(m.UnresolvedResources) {
		idx := len(m.Resources)
		res := m.UnresolvedResources[idx]
		var val internal.Value
		switch res := res.(type) {
		case program.Constant:
			val = res.Inner
			if val.GetType() == internal.TypeAccount {
				involvedAccountsMap[internal.Address(idx)] = string(val.(internal.AccountAddress))
			}
		case program.Variable:
			var ok bool
			val, ok = m.Vars[res.Name]
			if !ok {
				return nil, nil, fmt.Errorf("missing variable '%s'", res.Name)
			}
			if val.GetType() == internal.TypeAccount {
				involvedAccountsMap[internal.Address(idx)] = string(val.(internal.AccountAddress))
			}
		case program.VariableAccountMetadata:
			acc, _ := m.getResource(res.Account)
			addr := string((*acc).(internal.AccountAddress))

			metadata, err := store.GetMetadataFromLogs(ctx, addr, res.Key)
			if err != nil {
				return nil, nil, errorsutil.NewError(ErrResourceResolutionMissingMetadata, errors.New(
					fmt.Sprintf("missing key %v in metadata for account %s", res.Key, addr)))
			}

			val, err = internal.NewValueFromTypedJSON(metadata)
			if err != nil {
				return nil, nil, errorsutil.NewError(ErrResourceResolutionInvalidTypeFromExtSources, errors.New(
					fmt.Sprintf("invalid format for metadata at key %v for account %s", res.Key, addr)))
			}
		case program.VariableAccountBalance:
			acc, _ := m.getResource(res.Account)
			address := string((*acc).(internal.AccountAddress))
			involvedAccountsMap[internal.Address(idx)] = address
			m.UnresolvedResourceBalances[address] = idx

			ass, ok := m.getResource(res.Asset)
			if !ok {
				return nil, nil, fmt.Errorf(
					"variable '%s': tried to request account balance of an asset which has not yet been solved",
					res.Name)
			}
			if (*ass).GetType() != internal.TypeAsset {
				return nil, nil, fmt.Errorf(
					"variable '%s': tried to request account balance for an asset on wrong entity: %v instead of asset",
					res.Name, (*ass).GetType())
			}

			val = internal.Monetary{
				Asset: (*ass).(internal.Asset),
			}
		case program.Monetary:
			ass, _ := m.getResource(res.Asset)
			val = internal.Monetary{
				Asset:  (*ass).(internal.Asset),
				Amount: res.Amount,
			}
		default:
			panic(fmt.Errorf("type %T not implemented", res))
		}
		m.Resources = append(m.Resources, val)
	}

	involvedAccounts := make([]string, 0)
	involvedSources := make([]string, 0)
	for _, accountAddress := range involvedAccountsMap {
		involvedAccounts = append(involvedAccounts, accountAddress)
	}
	for _, machineAddress := range m.Program.Sources {
		involvedSources = append(involvedSources, involvedAccountsMap[machineAddress])
	}

	return involvedAccounts, involvedSources, nil
}

// TODO(gfyrag): Maybe rename to ResolveVars. Lifecycle seems to be ResolveVars -> ResolveResources -> ResolveBalances
func (m *Machine) SetVars(vars map[string]internal.Value) error {
	v, err := m.Program.ParseVariables(vars)
	if err != nil {
		return errorsutil.NewError(ErrInvalidVars, err)
	}
	m.Vars = v
	return nil
}

func (m *Machine) SetVarsFromJSON(vars map[string]json.RawMessage) error {
	v, err := m.Program.ParseVariablesJSON(vars)
	if err != nil {
		return errorsutil.NewError(ErrInvalidVars, err)
	}
	m.Vars = v
	return nil
}
