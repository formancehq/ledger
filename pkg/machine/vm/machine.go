package vm

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/numary/ledger/pkg/machine/internal"
	"github.com/numary/ledger/pkg/machine/vm/program"
	"github.com/numary/stack/libs/go-libs/metadata"
)

type Posting struct {
	Source      string                `json:"source"`
	Destination string                `json:"destination"`
	Amount      *internal.MonetaryInt `json:"amount"`
	Asset       string                `json:"asset"`
}

const InternalError = "internal interpreter error, please report to the issue tracker"

type Machine struct {
	store       Store
	ctx         context.Context
	vars        map[string]internal.Value
	balances    map[internal.AccountAddress]map[internal.Asset]internal.Number
	Postings    []Posting
	TxMeta      map[string]internal.Value
	AccountMeta map[internal.AccountAddress]map[string]internal.Value
	Printed     []internal.Value
}

func NewMachine(store Store) Machine {
	return Machine{
		store:       store,
		vars:        make(map[string]internal.Value),
		balances:    make(map[internal.AccountAddress]map[internal.Asset]internal.Number),
		Postings:    make([]Posting, 0),
		TxMeta:      make(map[string]internal.Value),
		AccountMeta: make(map[internal.AccountAddress]map[string]internal.Value),
		Printed:     make([]internal.Value, 0),
	}
}

func (m *Machine) checkVar(value internal.Value) error {
	switch v := value.(type) {
	case internal.Monetary:
		if v.Amount.Ltz() {
			return fmt.Errorf("monetary amounts must be non-negative but is %v", v.Amount)
		}
	}
	return nil
}

func (m *Machine) Execute(prog program.Program, providedVars map[string]string) error {
	for _, varDecl := range prog.VarsDecl {
		switch o := varDecl.Origin.(type) {
		case program.VarOriginMeta:
			account, err := EvalAs[internal.AccountAddress](m, o.Account)
			if err != nil {
				return err
			}
			metadata, err := m.store.GetMetadataFromLogs(m.ctx, string(*account), o.Key)
			if err != nil {
				return fmt.Errorf("failed to get metadata of account %s for key %s: %s", account, o.Key, err)
			}
			value, err := internal.NewValueFromString(varDecl.Typ, metadata)
			if err != nil {
				return fmt.Errorf("failed to parse variable: %s", err)
			}
			err = m.checkVar(value)
			if err != nil {
				return fmt.Errorf("failed to get metadata of account %s for key %s: %s", account, o.Key, err)
			}
			m.vars[varDecl.Name] = value
		case program.VarOriginBalance:
			account, err := EvalAs[internal.AccountAddress](m, o.Account)
			if err != nil {
				return err
			}
			asset, err := EvalAs[internal.Asset](m, o.Asset)
			if err != nil {
				return err
			}
			amt, err := m.store.GetBalanceFromLogs(m.ctx, string(*account), string(*asset))
			if err != nil {
				return err
			}
			var newAmt big.Int
			newAmt.Set(amt)
			balance := internal.Monetary{
				Asset:  *asset,
				Amount: internal.NewMonetaryIntFromBigInt(&newAmt),
			}
			err = m.checkVar(balance)
			if err != nil {
				return fmt.Errorf("failed to get balance of account %s for asset %s: %s", account, asset, err)
			}
			m.vars[varDecl.Name] = balance
		case nil:
			if _, ok := providedVars[varDecl.Name]; !ok {
				return fmt.Errorf("missing variable $%v", varDecl.Name)
			}
			val, err := internal.NewValueFromString(varDecl.Typ, providedVars[varDecl.Name])
			delete(providedVars, varDecl.Name)
			if err != nil {
				return fmt.Errorf("failed to parse variable: %s", err)
			}
			err = m.checkVar(val)
			if err != nil {
				return fmt.Errorf("variable passed is incorrect: %s", err)
			}
			m.vars[varDecl.Name] = val
		default:
			return errors.New(InternalError)
		}
	}

	if len(providedVars) > 0 {
		for p := range providedVars {
			return fmt.Errorf("extraneous variable $%v", p)
		}
	}

	for _, stmt := range prog.Instruction {
		switch s := stmt.(type) {
		case program.InstructionFail:
			return errors.New("failed")
		case program.InstructionPrint:
			v, err := m.Eval(s.Expr)
			if err != nil {
				return err
			}
			m.Printed = append(m.Printed, v)
			fmt.Printf("%v\n", s.Expr)

		case program.InstructionSave:
			account, err := EvalAs[internal.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			amt, err := EvalAs[internal.Monetary](m, s.Amount)
			if err != nil {
				return err
			}
			bal, err := m.BalanceOf(*account, amt.Asset)
			if err != nil {
				return err
			}
			*bal = *bal.Sub(amt.Amount)
		case program.InstructionSaveAll:
			account, err := EvalAs[internal.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			asset, err := EvalAs[internal.Asset](m, s.Asset)
			if err != nil {
				return err
			}
			bal, err := m.BalanceOf(*account, *asset)
			if err != nil {
				return err
			}
			*bal = *internal.Zero

		case program.InstructionAllocate:
			funding, err := EvalAs[internal.Funding](m, s.Funding)
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
			account, err := EvalAs[internal.AccountAddress](m, s.Account)
			if err != nil {
				return err
			}
			value, err := m.Eval(s.Value)
			if err != nil {
				return err
			}
			if _, ok := m.AccountMeta[*account]; !ok {
				m.AccountMeta[*account] = make(map[string]internal.Value)
			}
			m.AccountMeta[*account][s.Key] = value
		default:
			return errors.New(InternalError)
		}
	}
	return nil
}

func (m *Machine) Send(funding internal.Funding, account internal.AccountAddress) error {
	if funding.Total().Eq(internal.NewNumber(0)) {
		return nil //no empty postings
	}
	for _, part := range funding.Parts {
		m.Postings = append(m.Postings, Posting{
			Source:      string(part.Account),
			Destination: string(account),
			Asset:       string(funding.Asset),
			Amount:      part.Amount,
		})
		bal, err := m.BalanceOf(account, funding.Asset)
		if err != nil {
			return err
		}
		*bal = *bal.Add(part.Amount)
	}
	return nil
}

// Allocates a funding to a destination
// Part of the funding might be kept, and returned
// The kept part will always be the end of the original funding
func (m *Machine) Allocate(funding internal.Funding, destination program.Destination) (*internal.Funding, error) {
	kept := internal.Funding{
		Asset: funding.Asset,
	}
	switch d := destination.(type) {
	case program.DestinationAccount:
		account, err := EvalAs[internal.AccountAddress](m, d.Expr)
		if err != nil {
			return nil, err
		}
		m.Send(funding, *account)

	case program.DestinationInOrder:
		for _, part := range d.Parts {
			max, err := EvalAs[internal.Monetary](m, part.Max)
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

			var resultingKept internal.Funding
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
		portions := make([]internal.Portion, 0)
		subDests := make([]program.KeptOrDestination, 0)
		for _, part := range d {
			if part.Portion.Remaining {
				portions = append(portions, internal.NewPortionRemaining())
			} else {
				portion, err := EvalAs[internal.Portion](m, part.Portion.Expr)
				if err != nil {
					return nil, err
				}
				portions = append(portions, *portion)
			}
			subDests = append(subDests, part.Kod)
		}
		allotment, err := internal.NewAllotment(portions)
		if err != nil {
			return nil, fmt.Errorf("failed to create allotment: %v", err)
		}
		for i, part := range allotment.Allocate(funding.Total()) {
			taken, remainder, err := funding.Take(part)
			if err != nil {
				return nil, fmt.Errorf("failed to allocate to destination: %v", err)
			}
			kept, err := m.AllocateOrKeep(&taken, subDests[i])
			if err != nil {
				return nil, err
			}
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
func (m *Machine) AllocateOrKeep(funding *internal.Funding, kod program.KeptOrDestination) (kept *internal.Funding, err error) {
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

func (m *Machine) TakeFromValueAwareSource(source program.ValueAwareSource, mon internal.Monetary) (*internal.Funding, error) {
	switch s := source.(type) {
	case program.ValueAwareSourceSource:
		available, fallback, err := m.TakeFromSource(s.Source, mon.Asset)
		if err != nil {
			return nil, fmt.Errorf("failed to take from source: %v", err)
		}
		taken, remainder := available.TakeMax(mon.Amount)
		if !taken.Total().Eq(mon.Amount) {
			missing := internal.Monetary{
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
		portions := make([]internal.Portion, 0)
		for _, part := range s {
			if part.Portion.Remaining {
				portions = append(portions, internal.NewPortionRemaining())
			} else {
				portion, err := EvalAs[internal.Portion](m, part.Portion.Expr)
				if err != nil {
					return nil, err
				}
				portions = append(portions, *portion)
			}
		}
		allotment, err := internal.NewAllotment(portions)
		if err != nil {
			return nil, fmt.Errorf("could not create allotment: %v", err)
		}
		funding := internal.Funding{
			Asset: mon.Asset,
			Parts: make([]internal.FundingPart, 0),
		}
		for i, amt := range allotment.Allocate(mon.Amount) {
			taken, err := m.TakeFromValueAwareSource(program.ValueAwareSourceSource{Source: s[i].Source}, internal.Monetary{Asset: mon.Asset, Amount: amt})
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
	return nil, errors.New(InternalError)
}

func (m *Machine) TakeFromSource(source program.Source, asset internal.Asset) (*internal.Funding, *internal.AccountAddress, error) {
	switch s := source.(type) {
	case program.SourceAccount:
		account, err := EvalAs[internal.AccountAddress](m, s.Account)
		if err != nil {
			return nil, nil, err
		}
		overdraft := internal.Monetary{
			Asset:  asset,
			Amount: internal.NewNumber(0),
		}
		var fallback *internal.AccountAddress
		if s.Overdraft != nil {
			if s.Overdraft.Unbounded {
				fallback = account
			} else {
				ov, err := EvalAs[internal.Monetary](m, *s.Overdraft.UpTo)
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
		max, err := EvalAs[internal.Monetary](m, s.Max)
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
				missing := internal.Monetary{
					Asset:  asset,
					Amount: max.Amount.Sub(maxed.Total()),
				}
				withdrawn, err := m.WithdrawAlways(*fallback, missing)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to withdraw %s", err)
				}
				maxed, err = maxed.Concat(*withdrawn)
				if err != nil {
					return nil, nil, fmt.Errorf("funding error: %v", err)
				}
			}
		}
		return &maxed, nil, nil
	case program.SourceInOrder:
		total := internal.Funding{
			Asset: asset,
			Parts: make([]internal.FundingPart, 0),
		}
		var fallback *internal.AccountAddress
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
	return nil, nil, errors.New(InternalError)
}

func (m *Machine) Repay(funding internal.Funding) error {
	for _, part := range funding.Parts {
		balance, err := m.BalanceOf(part.Account, funding.Asset)
		if err != nil {
			return err
		}
		*balance = *balance.Add(part.Amount)
	}
	return nil
}

func (m *Machine) WithdrawAll(account internal.AccountAddress, asset internal.Asset, overdraft internal.Number) (*internal.Funding, error) {
	balance, err := m.BalanceOf(account, asset)
	if err != nil {
		return nil, fmt.Errorf("failed to withdraw %s", err)
	}
	amountTaken := internal.Zero
	balanceWithOverdraft := balance.Add(overdraft)
	if balanceWithOverdraft.Gt(internal.Zero) {
		amountTaken = balanceWithOverdraft
		*balance = *overdraft.Neg()
	}
	return &internal.Funding{
		Asset: asset,
		Parts: []internal.FundingPart{
			{
				Account: account,
				Amount:  amountTaken,
			},
		},
	}, nil
}

func (m *Machine) WithdrawAlways(account internal.AccountAddress, mon internal.Monetary) (*internal.Funding, error) {
	balance, err := m.BalanceOf(account, mon.Asset)
	if err != nil {
		return nil, err
	}
	*balance = *balance.Sub(mon.Amount)
	return &internal.Funding{
		Asset: mon.Asset,
		Parts: []internal.FundingPart{
			{
				Account: account,
				Amount:  mon.Amount,
			},
		},
	}, nil
}

func (m *Machine) BalanceOf(account internal.AccountAddress, asset internal.Asset) (internal.Number, error) {
	if _, ok := m.balances[account]; !ok {
		m.balances[account] = make(map[internal.Asset]internal.Number)
	}
	if _, ok := m.balances[account][asset]; !ok {
		amt, err := m.store.GetBalanceFromLogs(m.ctx, string(account), string(asset))
		if err != nil {
			return nil, fmt.Errorf("failed to get balance from store: %s", err)
		}
		m.balances[account][asset] = internal.NewMonetaryIntFromBigInt(amt)
	}
	return m.balances[account][asset], nil
}

func (m *Machine) Eval(expr program.Expr) (internal.Value, error) {
	switch expr := expr.(type) {
	case program.ExprLiteral:
		return expr.Value, nil
	case program.ExprNumberOperation:
		lhs, err := EvalAs[internal.Number](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[internal.Number](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case program.OP_ADD:
			return (*lhs).Add(*rhs), nil
		case program.OP_SUB:
			return (*lhs).Sub(*rhs), nil
		default:
			return nil, errors.New(InternalError)
		}

	case program.ExprMonetaryOperation:
		lhs, err := EvalAs[internal.Monetary](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[internal.Monetary](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		if lhs.Asset != rhs.Asset {
			return nil, errors.New("mismatching assets")
		}
		switch expr.Op {
		case program.OP_ADD:
			return internal.Monetary{
				Asset:  lhs.Asset,
				Amount: lhs.Amount.Add(rhs.Amount),
			}, nil
		case program.OP_SUB:
			return internal.Monetary{
				Asset:  lhs.Asset,
				Amount: lhs.Amount.Sub(rhs.Amount),
			}, nil
		default:
			return nil, errors.New(InternalError)
		}

	case program.ExprNumberCondition:
		lhs, err := EvalAs[internal.Number](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[internal.Number](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case program.OP_EQ:
			return internal.Bool((*lhs).Eq(*rhs)), nil
		case program.OP_NEQ:
			return internal.Bool(!(*lhs).Eq(*rhs)), nil
		case program.OP_LT:
			return internal.Bool((*lhs).Lt(*rhs)), nil
		case program.OP_LTE:
			return internal.Bool((*lhs).Lte(*rhs)), nil
		case program.OP_GT:
			return internal.Bool((*lhs).Gt(*rhs)), nil
		case program.OP_GTE:
			return internal.Bool((*lhs).Gte(*rhs)), nil
		}

	case program.ExprLogicalNot:
		operand, err := EvalAs[internal.Bool](m, expr.Operand)
		if err != nil {
			return nil, err
		}
		return internal.Bool(!bool(*operand)), nil
	case program.ExprLogicalAnd:
		lhs, err := EvalAs[internal.Bool](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[internal.Bool](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		return internal.Bool(bool(*lhs) && bool(*rhs)), nil
	case program.ExprLogicalOr:
		lhs, err := EvalAs[internal.Bool](m, expr.Lhs)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalAs[internal.Bool](m, expr.Rhs)
		if err != nil {
			return nil, err
		}
		return internal.Bool(bool(*lhs) || bool(*rhs)), nil

	case program.ExprMonetaryNew:
		asset, err := EvalAs[internal.Asset](m, expr.Asset)
		if err != nil {
			return nil, err
		}
		amount, err := EvalAs[internal.Number](m, expr.Amount)
		if err != nil {
			return nil, err
		}
		return internal.Monetary{
			Asset:  *asset,
			Amount: *amount,
		}, nil
	case program.ExprVariable:
		return m.vars[string(expr)], nil
	case program.ExprTake:
		amt, err := EvalAs[internal.Monetary](m, expr.Amount)
		if err != nil {
			return nil, err
		}
		taken, err := m.TakeFromValueAwareSource(expr.Source, *amt)
		if err != nil {
			return nil, fmt.Errorf("failed to take from source: %v", err)
		}
		return *taken, nil
	case program.ExprTakeAll:
		asset, err := EvalAs[internal.Asset](m, expr.Asset)
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
		cond, err := EvalAs[internal.Bool](m, expr.Cond)
		if err != nil {
			return nil, err
		}
		if bool(*cond) {
			return m.Eval(expr.IfTrue)
		} else {
			return m.Eval(expr.IfFalse)
		}
	}
	return nil, errors.New(InternalError)
}

func EvalAs[T internal.Value](i *Machine, expr program.Expr) (*T, error) {
	x, err := i.Eval(expr)
	if err != nil {
		return nil, err
	}
	if v, ok := x.(T); ok {
		return &v, nil
	}
	return nil, fmt.Errorf("internal interpreter error: expected type '%T' and got '%T'", *new(T), x)
}

func (m *Machine) GetTxMetaJSON() metadata.Metadata {
	meta := make(metadata.Metadata)
	for k, v := range m.TxMeta {
		var err error
		meta[k], err = internal.NewStringFromValue(v)
		if err != nil {
			panic(err)
		}
	}
	return meta
}

func (m *Machine) GetAccountsMetaJSON() map[string]metadata.Metadata {
	res := make(map[string]metadata.Metadata)
	for account, meta := range m.AccountMeta {
		for k, v := range meta {
			if _, ok := res[string(account)]; !ok {
				res[string(account)] = metadata.Metadata{}
			}

			var err error
			res[string(account)][k], err = internal.NewStringFromValue(v)
			if err != nil {
				panic(err)
			}
		}
	}

	return res
}
