package vm

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	"github.com/formancehq/ledger/internal/machine/vm/program"
)

// negExpr evaluates to [COIN -90]: monetary subtraction is the only way to
// build a negative monetary, every other entry point is guarded.
const negExpr = `[COIN 10] - [COIN 100]`

// run compiles and executes src against the given COIN balances.
func run(t *testing.T, src string, balances map[string]int64) ([]Posting, error) {
	t.Helper()
	p, err := compiler.Compile(src)
	require.NoError(t, err, "compile error")
	return runProgram(t, p, balances)
}

func runProgram(t *testing.T, p *program.Program, balances map[string]int64) ([]Posting, error) {
	t.Helper()

	m := NewMachine(*p)
	m.Printer = func(c chan machine.Value) {
		for range c {
		}
	}

	store := StaticStore{}
	for acc, bal := range balances {
		store[acc] = &AccountWithBalances{
			Account:  ledger.Account{Address: acc},
			Balances: map[string]*big.Int{"COIN": big.NewInt(bal)},
		}
	}

	if err := m.SetVarsFromJSON(map[string]string{}); err != nil {
		return nil, err
	}
	if err := m.ResolveResources(context.Background(), store); err != nil {
		return nil, err
	}
	if err := m.ResolveBalances(context.Background(), store); err != nil {
		return nil, err
	}
	if err := m.Execute(); err != nil {
		return nil, err
	}
	return m.Postings, nil
}

// ---------------------------------------------------------------------------
// 1. constructing a negative monetary
// ---------------------------------------------------------------------------

// A negative literal is not lexable: `monetary` is `[ <asset> NUMBER ]` and
// NUMBER is [0-9]+.
func TestNegativeMonetaryLiteralIsNotParseable(t *testing.T) {
	_, err := compiler.Compile(`send [COIN -100] (
		source = @world
		destination = @bob
	)`)
	require.Error(t, err)
}

// A negative balance read through balance() is rejected up-front...
func TestNegativeBalanceOriginIsRejected(t *testing.T) {
	p, err := compiler.Compile(`vars {
		monetary $bal = balance(@alice, COIN)
	}
	send $bal (
		source = @world
		destination = @bob
	)`)
	require.NoError(t, err)

	_, err = runProgram(t, p, map[string]int64{"alice": -100})
	require.ErrorIs(t, err, &machine.ErrNegativeAmount{})
	require.ErrorContains(t, err,
		"tried to request the balance of account alice for asset COIN: received -100: monetary amounts must be non-negative")
}

// ...but a negative balance on an account merely used as a *source* is not
// rejected: it is loaded as-is and simply yields nothing to withdraw.
func TestNegativeBalanceAsSourceIsNotRejected(t *testing.T) {
	_, err := run(t, `send [COIN 1] (
		source = @alice
		destination = @bob
	)`, map[string]int64{"alice": -100})
	require.ErrorIs(t, err, &machine.ErrInsufficientFund{})
}

// OP_MONETARY_SUB has no guard, so a negative monetary value is constructible
// at runtime even though no entry point accepts one.
func TestNegativeMonetaryCanBeBuiltBySubtraction(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `print `+negExpr)
	tc.expected = CaseResult{
		Printed: []machine.Value{machine.Monetary{
			Asset:  "COIN",
			Amount: machine.NewMonetaryInt(-90),
		}},
		Postings: []Posting{},
	}
	test(t, tc)
}

// ---------------------------------------------------------------------------
// 2. a negative send: the error depends on the opcode the source compiles to
// ---------------------------------------------------------------------------

// Sources that compile to OP_TAKE_MAX are guarded (machine.go, OP_TAKE_MAX).
func TestNegativeSendGuardedPaths(t *testing.T) {
	for _, tt := range []struct {
		name     string
		src      string
		balances map[string]int64
	}{
		{"world", `send ` + negExpr + ` (
			source = @world
			destination = @bob
		)`, nil},
		{"unbounded overdraft", `send ` + negExpr + ` (
			source = @alice allowing unbounded overdraft
			destination = @bob
		)`, nil},
		{"max <negative> from", `send [COIN 100] (
			source = max ` + negExpr + ` from @alice
			destination = @bob
		)`, map[string]int64{"alice": 1000}},
		{"destination max <negative>", `send [COIN 100] (
			source = @alice
			destination = {
				max ` + negExpr + ` to @bob
				remaining to @charlie
			}
		)`, map[string]int64{"alice": 1000}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := run(t, tt.src, tt.balances)
			require.ErrorContains(t, err, "cannot send a monetary with a negative amount: [COIN -90]")
		})
	}
}

// BUG: every bounded source compiles to OP_TAKE, which has no guard.
// funding.Take() cannot satisfy a negative amount, so it falls through to the
// insufficient-funds branch. The error is ALWAYS the same regardless of the
// balance (positive, zero or negative), the overdraft allowance, or the source
// and destination shapes -- and the API maps it to HTTP 400 INSUFFICIENT_FUND.
func TestNegativeSendAlwaysReportsInsufficientFunds(t *testing.T) {
	for _, tt := range []struct {
		name     string
		src      string
		balances map[string]int64
	}{
		{"positive balance", `send ` + negExpr + ` (
			source = @alice
			destination = @bob
		)`, map[string]int64{"alice": 1000}},
		{"zero balance", `send ` + negExpr + ` (
			source = @alice
			destination = @bob
		)`, map[string]int64{"alice": 0}},
		{"negative balance", `send ` + negExpr + ` (
			source = @alice
			destination = @bob
		)`, map[string]int64{"alice": -500}},
		{"bounded overdraft", `send ` + negExpr + ` (
			source = @alice allowing overdraft up to [COIN 1000]
			destination = @bob
		)`, map[string]int64{"alice": -500}},
		{"destination allotment", `send ` + negExpr + ` (
			source = @alice
			destination = {
				1/2 to @bob
				1/2 to @charlie
			}
		)`, map[string]int64{"alice": 1000}},
		{"destination in order", `send ` + negExpr + ` (
			source = @alice
			destination = {
				max [COIN 10] to @bob
				remaining to @charlie
			}
		)`, map[string]int64{"alice": 1000}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := run(t, tt.src, tt.balances)
			require.ErrorIs(t, err, &machine.ErrInsufficientFund{})
			require.ErrorContains(t, err, "account(s) @alice had/have insufficient funds")
		})
	}
}

// The accounts named in that error depend on how the negative amount is split,
// so the (already wrong) message is not even stable.
func TestNegativeSendErrorNamesArbitraryAccounts(t *testing.T) {
	// in-order: the whole pool is taken at once, both accounts are named.
	_, err := run(t, `send `+negExpr+` (
		source = {
			@alice
			@charlie
		}
		destination = @bob
	)`, map[string]int64{"alice": 1000, "charlie": 1000})
	require.ErrorContains(t, err, "account(s) @alice|@charlie had/have insufficient funds")

	// allotment: Allocate(-90) splits into [-45 -45], the first take fails.
	_, err = run(t, `send `+negExpr+` (
		source = {
			1/2 from @alice
			1/2 from @charlie
		}
		destination = @bob
	)`, map[string]int64{"alice": 1000, "charlie": 1000})
	require.ErrorContains(t, err, "account(s) @alice had/have insufficient funds")

	// Allocate(-1) floors to [-1 -1] then corrects to [0 -1], so @alice's leg
	// succeeds and only @charlie is blamed -- purely a rounding artefact.
	_, err = run(t, `send [COIN 0] - [COIN 1] (
		source = {
			1/2 from @alice
			1/2 from @charlie
		}
		destination = @bob
	)`, map[string]int64{"alice": 1000, "charlie": 1000})
	require.ErrorContains(t, err, "account(s) @charlie had/have insufficient funds")
}

// ---------------------------------------------------------------------------
// 3. the sibling case: zero-amount postings ARE emitted
// ---------------------------------------------------------------------------

// funding.Take() special-cases a zero amount by attaching a zero part, so a
// zero-amount posting is produced and persisted (Postings.Validate only
// rejects amounts < 0). The numscript v2 interpreter drops these.
func TestZeroAmountPostingIsEmitted(t *testing.T) {
	postings, err := run(t, `send [COIN 10] - [COIN 10] (
		source = @alice
		destination = @bob
	)`, map[string]int64{"alice": 1000})
	require.NoError(t, err)
	require.Equal(t, []Posting{{
		Source:      "alice",
		Destination: "bob",
		Asset:       "COIN",
		Amount:      machine.NewMonetaryInt(0),
	}}, postings)
}

// ...even when the source is insolvent and could not fund anything at all.
func TestZeroAmountPostingFromInsolventAccount(t *testing.T) {
	postings, err := run(t, `send [COIN 10] - [COIN 10] (
		source = @alice
		destination = @bob
	)`, map[string]int64{"alice": -500})
	require.NoError(t, err)
	require.Len(t, postings, 1)
	require.Equal(t, machine.NewMonetaryInt(0), postings[0].Amount)
}

// ---------------------------------------------------------------------------
// 4. `save` silently drops monetary arithmetic
// ---------------------------------------------------------------------------

// VisitSaveFromAccount evaluates the expression with push=false and then
// pushes the address VisitExpr returned, which for an add/sub is the address
// of the LEFT operand. So `save A - B` saves A and the subtraction is lost.
// (This is also why a negative `save` is unreachable: it degrades to its LHS.)
func TestSaveDropsMonetaryArithmetic(t *testing.T) {
	// alice has 100 and saves 100-60=40, so 60 should remain spendable.
	_, err := run(t, `save [COIN 100] - [COIN 60] from @alice
	send [COIN 50] (
		source = @alice
		destination = @bob
	)`, map[string]int64{"alice": 100})
	require.ErrorIs(t, err, &machine.ErrInsufficientFund{},
		"save [COIN 100] - [COIN 60] saved 100 instead of 40")

	// control: the same amount as a literal behaves correctly.
	postings, err := run(t, `save [COIN 40] from @alice
	send [COIN 50] (
		source = @alice
		destination = @bob
	)`, map[string]int64{"alice": 100})
	require.NoError(t, err)
	require.Equal(t, machine.NewMonetaryInt(50), postings[0].Amount)
}

// ---------------------------------------------------------------------------
// 5. the invariant that actually matters
// ---------------------------------------------------------------------------

// No script currently reaches OP_SEND with a negative funding part: every
// compiler path that can carry a user-controlled amount into OP_TAKE_ALWAYS
// goes through the OP_TAKE_MAX guard first. This asserts that end to end.
func TestNoScriptEmitsANegativePosting(t *testing.T) {
	srcs := []string{
		`send ` + negExpr + ` (
			source = @alice
			destination = @bob
		)`,
		`send ` + negExpr + ` (
			source = @world
			destination = @bob
		)`,
		`send ` + negExpr + ` (
			source = @alice allowing unbounded overdraft
			destination = @bob
		)`,
		`send ` + negExpr + ` (
			source = {
				@alice
				@world
			}
			destination = @bob
		)`,
		`send ` + negExpr + ` (
			source = {
				1/2 from @alice
				1/2 from @world
			}
			destination = @bob
		)`,
		`send [COIN 100] (
			source = max ` + negExpr + ` from @alice
			destination = @bob
		)`,
		`send [COIN 100] (
			source = @alice
			destination = {
				max ` + negExpr + ` to @bob
				remaining to @charlie
			}
		)`,
	}
	for i, src := range srcs {
		postings, err := run(t, src, map[string]int64{"alice": 1000, "charlie": 1000})
		if err != nil {
			continue // rejected, which is the point
		}
		for _, p := range postings {
			require.False(t, p.Amount.Ltz(), "script %d emitted a negative posting: %v", i, p)
		}
	}
}

// ...but nothing in the VM enforces it. OP_TAKE_ALWAYS happily builds a
// negative funding part and OP_SEND turns it straight into a negative posting,
// so the invariant rests entirely on the compiler picking guarded opcodes.
// The v2 interpreter has an explicit checkPostingInvariants for exactly this.
func TestVMHasNoNegativePostingGuard(t *testing.T) {
	apush := func(addr uint16) []byte {
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, addr)
		return append([]byte{program.OP_APUSH}, b...)
	}

	instructions := append([]byte{}, apush(0)...)    // @alice
	instructions = append(instructions, apush(1)...) // [COIN -90]
	instructions = append(instructions, program.OP_TAKE_ALWAYS)
	instructions = append(instructions, apush(2)...) // @bob
	instructions = append(instructions, program.OP_SEND)

	p := &program.Program{
		Instructions: instructions,
		Resources: []program.Resource{
			program.Constant{Inner: machine.AccountAddress("alice")},
			program.Constant{Inner: machine.Monetary{
				Asset:  "COIN",
				Amount: machine.NewMonetaryInt(-90),
			}},
			program.Constant{Inner: machine.AccountAddress("bob")},
		},
	}

	postings, err := runProgram(t, p, nil)
	require.NoError(t, err)
	require.Equal(t, []Posting{{
		Source:      "alice",
		Destination: "bob",
		Asset:       "COIN",
		Amount:      machine.NewMonetaryInt(-90),
	}}, postings, "the VM emitted a negative posting without complaint")
}

// The one place a negative monetary does reach persisted state: metadata.
// The transaction itself is valid, so it commits, carrying "COIN -90" into
// the tx and account metadata columns.
func TestNegativeMonetaryReachesMetadata(t *testing.T) {
	p, err := compiler.Compile(`send [COIN 100] (
		source = @alice
		destination = @bob
	)
	set_tx_meta("fee", ` + negExpr + `)
	set_account_meta(@bob, "adj", ` + negExpr + `)`)
	require.NoError(t, err)

	m := NewMachine(*p)
	m.Printer = func(c chan machine.Value) {
		for range c {
		}
	}
	store := StaticStore{"alice": &AccountWithBalances{
		Account:  ledger.Account{Address: "alice"},
		Balances: map[string]*big.Int{"COIN": big.NewInt(1000)},
	}}
	require.NoError(t, m.SetVarsFromJSON(map[string]string{}))
	require.NoError(t, m.ResolveResources(context.Background(), store))
	require.NoError(t, m.ResolveBalances(context.Background(), store))
	require.NoError(t, m.Execute())

	require.Equal(t, metadata.Metadata{"fee": "COIN -90"}, m.GetTxMetaJSON())
	require.Equal(t, map[string]metadata.Metadata{
		"bob": {"adj": "COIN -90"},
	}, m.GetAccountsMetaJSON())
}
