package vm

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/vm/program"
)

type Posting struct {
	Source      string            `json:"source"`
	Destination string            `json:"destination"`
	Amount      *core.MonetaryInt `json:"amount"`
	Asset       string            `json:"asset"`
}

const coreError = "core interpreter error, please report to the issue tracker"

type Machine struct {
	program     program.Program
	vars        map[string]core.Value
	balances    map[core.AccountAddress]map[core.Asset]*core.MonetaryInt
	Postings    []Posting
	TxMeta      map[string]core.Value
	AccountMeta map[core.AccountAddress]map[string]core.Value
	Printed     []core.Value
}

func NewMachine(program program.Program) Machine {
	return Machine{
		program:     program,
		vars:        make(map[string]core.Value),
		balances:    make(map[core.AccountAddress]map[core.Asset]*core.MonetaryInt),
		Postings:    make([]Posting, 0),
		TxMeta:      make(map[string]core.Value),
		AccountMeta: make(map[core.AccountAddress]map[string]core.Value),
		Printed:     make([]core.Value, 0),
	}
}

func (m *Machine) checkVar(value core.Value) error {
	switch v := value.(type) {
	case core.Monetary:
		if v.Amount.Ltz() {
			return fmt.Errorf("monetary amounts must be non-negative but is %v", v.Amount)
		}
	}
	return nil
}

func (m *Machine) SetVars(vars map[string]core.Value) error {
	v, err := m.program.ParseVariables(vars)
	if err != nil {
		return err
	}
	m.vars = v
	return nil
}

func (m *Machine) SetVarsFromJSON(vars map[string]json.RawMessage) error {
	v, err := m.program.ParseVariablesJSON(vars)
	if err != nil {
		return err
	}
	m.vars = v
	return nil
}

func (m *Machine) ResolveResources(getMetadata func(core.AccountAddress, string) (*core.Value, error), getBalance func(core.AccountAddress, core.Asset) (*core.MonetaryInt, error)) error {
	// Resolving Variables
	for _, varDecl := range m.program.VarsDecl {
		switch o := varDecl.Origin.(type) {
		case program.VarOriginMeta:
			account, err := EvalAs[core.AccountAddress](m, o.Account)
			if err != nil {
				return err
			}
			value, err := getMetadata(*account, o.Key)
			if err != nil {
				return fmt.Errorf("failed to get metadata of account %s for key %s: %s", account, o.Key, err)
			}
			err = m.checkVar(*value)
			if err != nil {
				return fmt.Errorf("failed to get metadata of account %s for key %s: %s", account, o.Key, err)
			}
			m.vars[varDecl.Name] = *value
		case program.VarOriginBalance:
			account, err := EvalAs[core.AccountAddress](m, o.Account)
			if err != nil {
				return err
			}
			asset, err := EvalAs[core.Asset](m, o.Asset)
			if err != nil {
				return err
			}
			amt, err := getBalance(*account, *asset)
			if err != nil {
				return err
			}
			balance := core.Monetary{
				Asset:  *asset,
				Amount: amt,
			}
			err = m.checkVar(balance)
			if err != nil {
				return fmt.Errorf("failed to get balance of account %s for asset %s: %s", account, asset, err)
			}
			m.vars[varDecl.Name] = balance
		case nil:
			if _, ok := m.vars[varDecl.Name]; !ok {
				return fmt.Errorf("missing variable $%v", varDecl.Name)
			}
			// val, err := core.NewValueFromString(varDecl.Typ, providedVars[varDecl.Name])
			// delete(providedVars, varDecl.Name)
			// if err != nil {
			// 	return fmt.Errorf("failed to parse variable: %s", err)
			// }
			// err = m.checkVar(val)
			// if err != nil {
			// 	return fmt.Errorf("variable passed is incorrect: %s", err)
			// }
			// m.vars[varDecl.Name] = val
		default:
			return errors.New(coreError)
		}
	}

	// Resolving balances
	err := m.resolveBalances(getBalance)
	if err != nil {
		return err
	}

	return nil
}

// func addNeededBalances() {

// }

func (m *Machine) Execute() error {
	for _, stmt := range m.program.Instruction {
		switch s := stmt.(type) {
		case program.InstructionFail:
			return errors.New("failed")
		case program.InstructionPrint:
			v, err := m.Eval(s.Expr)
			if err != nil {
				return err
			}
			m.Printed = append(m.Printed, v)

		case program.InstructionSave:
			account, err := EvalAs[core.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			amt, err := EvalAs[core.Monetary](m, s.Amount)
			if err != nil {
				return err
			}
			bal, err := m.BalanceOf(*account, amt.Asset)
			if err != nil {
				return err
			}
			*bal = *bal.Sub(amt.Amount)
		case program.InstructionSaveAll:
			account, err := EvalAs[core.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			asset, err := EvalAs[core.Asset](m, s.Asset)
			if err != nil {
				return err
			}
			bal, err := m.BalanceOf(*account, *asset)
			if err != nil {
				return err
			}
			*bal = *core.NewMonetaryInt(0)

		case program.InstructionAllocate:
			funding, err := EvalAs[core.Funding](m, s.Funding)
			if err != nil {
				return err
			}
			kept, err := m.Allocate(*funding, s.Destination)
			if err != nil {
				return err
			}
			m.Repay(*kept)
		case program.InstructionSetTxMeta:
			value, err := m.Eval(s.Value)
			if err != nil {
				return err
			}
			m.TxMeta[s.Key] = value
		case program.InstructionSetAccountMeta:
			account, err := EvalAs[core.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			value, err := m.Eval(s.Value)
			if err != nil {
				return err
			}
			if _, ok := m.AccountMeta[*account]; !ok {
				m.AccountMeta[*account] = make(map[string]core.Value)
			}
			m.AccountMeta[*account][s.Key] = value
		default:
			return errors.New(coreError)
		}
	}
	return nil
}

func (m *Machine) Send(funding core.Funding, account core.AccountAddress) error {
	if funding.Total().Eq(core.NewNumber(0)) {
		return nil //no empty postings
	}
	for _, part := range funding.Parts {
		m.Postings = append(m.Postings, Posting{
			Source:      string(part.Account),
			Destination: string(account),
			Asset:       string(funding.Asset),
			Amount:      part.Amount,
		})

		if bal, ok := m.balances[account][funding.Asset]; ok {
			m.balances[account][funding.Asset] = bal.Add(part.Amount)
		}
	}
	return nil
}

// Allocates a funding to a destination
// Part of the funding might be kept, and returned
// The kept part will always be the end of the original funding
func (m *Machine) Allocate(funding core.Funding, destination program.Destination) (*core.Funding, error) {
	kept := core.Funding{
		Asset: funding.Asset,
	}
	switch d := destination.(type) {
	case program.DestinationAccount:
		account, err := EvalAs[core.AccountAddress](m, d.Expr)
		if err != nil {
			return nil, err
		}
		err = m.Send(funding, *account)
		if err != nil {
			return nil, err
		}

	case program.DestinationInOrder:
		for _, part := range d.Parts {
			max, err := EvalAs[core.Monetary](m, part.Max)
			if err != nil {
				return nil, err
			}
			taken, remainder := funding.TakeMax(max.Amount)
			subdestKept, err := m.AllocateOrKeep(&taken, part.Kod)
			if err != nil {
				return nil, err
			}

			keptAmt := subdestKept.Total()
			funding, err = subdestKept.Concat(remainder)
			if err != nil {
				return nil, err
			}

			var resultingKept core.Funding
			resultingKept, funding, err = funding.TakeFromBottom(keptAmt)
			if err != nil {
				return nil, err
			}

			kept, err = resultingKept.Concat(kept)
			if err != nil {
				return nil, err
			}
		}
		subdestKept, err := m.AllocateOrKeep(&funding, d.Remaining)
		if err != nil {
			return nil, err
		}
		kept, err = subdestKept.Concat(kept)
		if err != nil {
			return nil, err
		}
	case program.DestinationAllotment:
		portions := make([]core.Portion, 0)
		subDests := make([]program.KeptOrDestination, 0)
		for _, part := range d {
			if part.Portion.Remaining {
				portions = append(portions, core.NewPortionRemaining())
			} else {
				portion, err := EvalAs[core.Portion](m, part.Portion.Expr)
				if err != nil {
					return nil, err
				}
				portions = append(portions, *portion)
			}
			subDests = append(subDests, part.Kod)
		}
		allotment, err := core.NewAllotment(portions)
		if err != nil {
			return nil, fmt.Errorf("failed to create allotment: %v", err)
		}
		for i, part := range allotment.Allocate(funding.Total()) {
			fmt.Printf("allocating %v\n", part)
			taken, remainder, err := funding.Take(part)
			if err != nil {
				return nil, fmt.Errorf("failed to allocate to destination: %v", err)
			}
			fmt.Printf("took and got %v (remainder is %v)\n", taken, remainder)
			kept, err := m.AllocateOrKeep(&taken, subDests[i])
			if err != nil {
				return nil, err
			}
			fmt.Printf("allocated or kept, kept %v\n", kept)
			funding, err = kept.Concat(remainder)
			if err != nil {
				return nil, err
			}
		}
		kept = funding
	}
	return &kept, nil
}

// Allocates a funding to a destination or keeps it entirely
// The kept part of the funding is returned
func (m *Machine) AllocateOrKeep(funding *core.Funding, kod program.KeptOrDestination) (kept *core.Funding, err error) {
	if kod.Kept {
		kept = funding
	} else {
		kept, err = m.Allocate(*funding, kod.Destination)
		if err != nil {
			return nil, err
		}
	}
	return kept, nil
}

func (m *Machine) TakeFromValueAwareSource(source program.ValueAwareSource, mon core.Monetary) (*core.Funding, error) {
	switch s := source.(type) {
	case program.ValueAwareSourceSource:
		available, fallback, err := m.TakeFromSource(s.Source, mon.Asset)
		if err != nil {
			return nil, fmt.Errorf("failed to take from source: %v", err)
		}
		taken, remainder := available.TakeMax(mon.Amount)
		if !taken.Total().Eq(mon.Amount) {
			missing := core.Monetary{
				Asset:  mon.Asset,
				Amount: mon.Amount.Sub(taken.Total()),
			}
			if fallback != nil {
				missingTaken, err := m.WithdrawAlways(*fallback, missing)
				if err != nil {
					return nil, err
				}
				taken, err = taken.Concat(*missingTaken)
				if err != nil {
					return nil, errors.New("mismatching assets")
				}
			} else {
				return nil, fmt.Errorf("insufficient funds: needed %v and got %v", mon.Amount, taken.Total())
			}
		}
		m.Repay(remainder)
		return &taken, nil
	case program.ValueAwareSourceAllotment:
		portions := make([]core.Portion, 0)
		for _, part := range s {
			if part.Portion.Remaining {
				portions = append(portions, core.NewPortionRemaining())
			} else {
				portion, err := EvalAs[core.Portion](m, part.Portion.Expr)
				if err != nil {
					return nil, err
				}
				portions = append(portions, *portion)
			}
		}
		allotment, err := core.NewAllotment(portions)
		if err != nil {
			return nil, fmt.Errorf("could not create allotment: %v", err)
		}
		funding := core.Funding{
			Asset: mon.Asset,
			Parts: make([]core.FundingPart, 0),
		}
		for i, amt := range allotment.Allocate(mon.Amount) {
			taken, err := m.TakeFromValueAwareSource(program.ValueAwareSourceSource{Source: s[i].Source}, core.Monetary{Asset: mon.Asset, Amount: amt})
			if err != nil {
				return nil, fmt.Errorf("failed to take from source: %v", err)
			}
			funding, err = funding.Concat(*taken)
			if err != nil {
				return nil, fmt.Errorf("funding error: %v", err)
			}
		}
		return &funding, nil
	}
	return nil, errors.New(coreError)
}

func (m *Machine) TakeFromSource(source program.Source, asset core.Asset) (*core.Funding, *core.AccountAddress, error) {
	switch s := source.(type) {
	case program.SourceAccount:
		account, err := EvalAs[core.AccountAddress](m, s.Account)
		if err != nil {
			return nil, nil, err
		}
		overdraft := core.Monetary{
			Asset:  asset,
			Amount: core.NewNumber(0),
		}
		var fallback *core.AccountAddress
		if s.Overdraft != nil {
			if s.Overdraft.Unbounded {
				fallback = account
			} else {
				ov, err := EvalAs[core.Monetary](m, *s.Overdraft.UpTo)
				if err != nil {
					return nil, nil, err
				}
				overdraft = *ov
			}
		}
		if string(*account) == "world" {
			fallback = account
		}
		if overdraft.Asset != asset {
			return nil, nil, errors.New("mismatching asset")
		}
		funding, err := m.WithdrawAll(*account, asset, overdraft.Amount)
		if err != nil {
			return nil, nil, err
		}
		return funding, fallback, nil
	case program.SourceMaxed:
		taken, fallback, err := m.TakeFromSource(s.Source, asset)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to take from source: %v", err)
		}
		max, err := EvalAs[core.Monetary](m, s.Max)
		if err != nil {
			return nil, nil, err
		}
		if max.Asset != asset {
			return nil, nil, errors.New("mismatching asset")
		}
		maxed, remainder := taken.TakeMax(max.Amount)
		m.Repay(remainder)
		if maxed.Total().Lte(max.Amount) {
			if fallback != nil {
				missing := core.Monetary{
					Asset:  asset,
					Amount: max.Amount.Sub(maxed.Total()),
				}
				withdrawn, err := m.WithdrawAlways(*fallback, missing)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to withdraw: %s", err)
				}
				maxed, err = maxed.Concat(*withdrawn)
				if err != nil {
					return nil, nil, fmt.Errorf("funding error: %v", err)
				}
			}
		}
		return &maxed, nil, nil
	case program.SourceInOrder:
		total := core.Funding{
			Asset: asset,
			Parts: make([]core.FundingPart, 0),
		}
		var fallback *core.AccountAddress
		nbSources := len(s)
		for i, source := range s {
			subsourceTaken, subsourceFallback, err := m.TakeFromSource(source, asset)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to take from source: %v", err)
			}
			if subsourceFallback != nil && i != nbSources-1 {
				return nil, nil, errors.New("fallback is not in the last position") // FIXME: shouldn't we let this slide?
			}
			fallback = subsourceFallback
			total, err = total.Concat(*subsourceTaken)
			if err != nil {
				return nil, nil, errors.New("mismatching assets")
			}
		}
		return &total, fallback, nil
	}
	return nil, nil, errors.New(coreError)
}

func (m *Machine) Repay(funding core.Funding) error {
	for _, part := range funding.Parts {
		balance, err := m.BalanceOf(part.Account, funding.Asset)
		if err != nil {
			return err
		}
		*balance = *balance.Add(part.Amount)
	}
	return nil
}

func (m *Machine) WithdrawAll(account core.AccountAddress, asset core.Asset, overdraft core.Number) (*core.Funding, error) {
	balance, err := m.BalanceOf(account, asset)
	if err != nil {
		return nil, fmt.Errorf("failed to withdraw: %s", err)
	}
	amountTaken := core.NewMonetaryInt(0)
	balanceWithOverdraft := balance.Add(overdraft)
	if balanceWithOverdraft.Gt(core.NewMonetaryInt(0)) {
		amountTaken = balanceWithOverdraft
		*balance = *overdraft.Neg()
	}
	return &core.Funding{
		Asset: asset,
		Parts: []core.FundingPart{
			{
				Account: account,
				Amount:  amountTaken,
			},
		},
	}, nil
}

func (m *Machine) WithdrawAlways(account core.AccountAddress, mon core.Monetary) (*core.Funding, error) {
	balance, err := m.BalanceOf(account, mon.Asset)
	if err != nil {
		return nil, err
	}
	*balance = *balance.Sub(mon.Amount)
	return &core.Funding{
		Asset: mon.Asset,
		Parts: []core.FundingPart{
			{
				Account: account,
				Amount:  mon.Amount,
			},
		},
	}, nil
}

func (m *Machine) BalanceOf(account core.AccountAddress, asset core.Asset) (core.Number, error) {
	if accBalances, ok := m.balances[account]; ok {
		if balance, ok := accBalances[asset]; ok {
			return balance, nil
		}
	}
	return nil, fmt.Errorf("missing %v balance from %v", asset, account)
}

func (m *Machine) Eval(expr program.Expr) (core.Value, error) {
	switch expr := expr.(type) {
	case program.ExprLiteral:
		return expr.Value, nil
	case program.ExprNumberOperation:
		lhs, err := EvalAs[core.Number](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[core.Number](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case program.OP_ADD:
			return (*lhs).Add(*rhs), nil
		case program.OP_SUB:
			return (*lhs).Sub(*rhs), nil
		default:
			return nil, errors.New(coreError)
		}

	case program.ExprMonetaryOperation:
		lhs, err := EvalAs[core.Monetary](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[core.Monetary](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		if lhs.Asset != rhs.Asset {
			return nil, errors.New("mismatching assets")
		}
		switch expr.Op {
		case program.OP_ADD:
			return core.Monetary{
				Asset:  lhs.Asset,
				Amount: lhs.Amount.Add(rhs.Amount),
			}, nil
		case program.OP_SUB:
			return core.Monetary{
				Asset:  lhs.Asset,
				Amount: lhs.Amount.Sub(rhs.Amount),
			}, nil
		default:
			return nil, errors.New(coreError)
		}

	case program.ExprNumberCondition:
		lhs, err := EvalAs[core.Number](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[core.Number](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case program.OP_EQ:
			return core.Bool((*lhs).Eq(*rhs)), nil
		case program.OP_NEQ:
			return core.Bool(!(*lhs).Eq(*rhs)), nil
		case program.OP_LT:
			return core.Bool((*lhs).Lt(*rhs)), nil
		case program.OP_LTE:
			return core.Bool((*lhs).Lte(*rhs)), nil
		case program.OP_GT:
			return core.Bool((*lhs).Gt(*rhs)), nil
		case program.OP_GTE:
			return core.Bool((*lhs).Gte(*rhs)), nil
		}

	case program.ExprLogicalNot:
		operand, err := EvalAs[core.Bool](m, expr.Operand)
		if err != nil {
			return nil, err
		}
		return core.Bool(!bool(*operand)), nil
	case program.ExprLogicalAnd:
		lhs, err := EvalAs[core.Bool](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[core.Bool](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		return core.Bool(bool(*lhs) && bool(*rhs)), nil
	case program.ExprLogicalOr:
		lhs, err := EvalAs[core.Bool](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[core.Bool](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		return core.Bool(bool(*lhs) || bool(*rhs)), nil

	case program.ExprMonetaryNew:
		asset, err := EvalAs[core.Asset](m, expr.Asset)
		if err != nil {
			return nil, err
		}
		amount, err := EvalAs[core.Number](m, expr.Amount)
		if err != nil {
			return nil, err
		}
		return core.Monetary{
			Asset:  *asset,
			Amount: *amount,
		}, nil
	case program.ExprVariable:
		return m.vars[string(expr)], nil
	case program.ExprTake:
		amt, err := EvalAs[core.Monetary](m, expr.Amount)
		if err != nil {
			return nil, err
		}
		taken, err := m.TakeFromValueAwareSource(expr.Source, *amt)
		if err != nil {
			return nil, fmt.Errorf("failed to take from source: %v", err)
		}
		return *taken, nil
	case program.ExprTakeAll:
		asset, err := EvalAs[core.Asset](m, expr.Asset)
		if err != nil {
			return nil, err
		}
		funding, fallback, err := m.TakeFromSource(expr.Source, *asset)
		if err != nil {
			return nil, fmt.Errorf("failed to take from source: %v", err)
		}
		if fallback != nil {
			panic("oops infinite money")
		}
		return *funding, nil
	case program.ExprTernary:
		cond, err := EvalAs[core.Bool](m, expr.Cond)
		if err != nil {
			return nil, err
		}
		if bool(*cond) {
			return m.Eval(expr.IfTrue)
		} else {
			return m.Eval(expr.IfFalse)
		}
	}
	return nil, errors.New(coreError)
}

func EvalAs[T core.Value](i *Machine, expr program.Expr) (*T, error) {
	x, err := i.Eval(expr)
	if err != nil {
		return nil, err
	}
	if v, ok := x.(T); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("core interpreter error: expected type '%T' and got '%T'", *new(T), x)
}

type Metadata map[string]any

func (m *Machine) GetTxMetaJSON() Metadata {
	meta := make(Metadata)
	for k, v := range m.TxMeta {
		valJSON, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		v, err := json.Marshal(core.ValueJSON{
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
	for account, meta := range m.AccountMeta {
		for k, v := range meta {
			if _, ok := res[account.String()]; !ok {
				res[account.String()] = map[string][]byte{}
			}
			valJSON, err := json.Marshal(v)
			if err != nil {
				panic(err)
			}
			v, err := json.Marshal(core.ValueJSON{
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
