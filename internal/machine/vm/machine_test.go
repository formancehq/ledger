package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/formancehq/ledger/internal/machine"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
	"github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	DEBUG bool = false
)

type CaseResult struct {
	Printed       []machine.Value
	Postings      []Posting
	Metadata      map[string]machine.Value
	Error         error
	ErrorContains string
}

type TestCase struct {
	program  *program.Program
	vars     map[string]string
	meta     map[string]metadata.Metadata
	balances map[string]map[string]*machine.MonetaryInt
	expected CaseResult
}

func NewTestCase() TestCase {
	return TestCase{
		vars:     make(map[string]string),
		meta:     make(map[string]metadata.Metadata),
		balances: make(map[string]map[string]*machine.MonetaryInt),
		expected: CaseResult{
			Printed:  []machine.Value{},
			Postings: []Posting{},
			Metadata: make(map[string]machine.Value),
			Error:    nil,
		},
	}
}

func (c *TestCase) compile(t *testing.T, code string) {
	p, err := compiler.Compile(code)
	if err != nil {
		t.Fatalf("compile error: %v", err)
		return
	}
	c.program = p
}

func (c *TestCase) setVarsFromJSON(t *testing.T, str string) {
	var jsonVars map[string]string
	err := json.Unmarshal([]byte(str), &jsonVars)
	require.NoError(t, err)
	c.vars = jsonVars
}

func (c *TestCase) setBalance(account, asset string, amount int64) {
	if _, ok := c.balances[account]; !ok {
		c.balances[account] = make(map[string]*machine.MonetaryInt)
	}
	c.balances[account][asset] = machine.NewMonetaryInt(amount)
}

func test(t *testing.T, testCase TestCase) {
	testImpl(t, testCase.program, testCase.expected, func(m *Machine) error {
		if err := m.SetVarsFromJSON(testCase.vars); err != nil {
			return err
		}

		store := StaticStore{}
		for account, balances := range testCase.balances {
			store[account] = &AccountWithBalances{
				Account: ledger.Account{
					Address:  account,
					Metadata: testCase.meta[account],
				},
				Balances: func() map[string]*big.Int {
					ret := make(map[string]*big.Int)
					for asset, balance := range balances {
						ret[asset] = (*big.Int)(balance)
					}
					return ret
				}(),
			}
		}

		_, _, err := m.ResolveResources(context.Background(), store)
		if err != nil {
			return err
		}

		err = m.ResolveBalances(context.Background(), store)
		if err != nil {
			return err
		}

		return m.Execute()
	})
}

func testImpl(t *testing.T, prog *program.Program, expected CaseResult, exec func(*Machine) error) {
	printed := []machine.Value{}

	var wg sync.WaitGroup
	wg.Add(1)

	require.NotNil(t, prog)

	m := NewMachine(*prog)
	m.Debug = DEBUG
	m.Printer = func(c chan machine.Value) {
		for v := range c {
			printed = append(printed, v)
		}
		wg.Done()
	}

	err := exec(m)
	if expected.Error != nil {
		require.True(t, errors.Is(err, expected.Error), "got wrong error, want: %v, got: %v", expected.Error, err)
		if expected.ErrorContains != "" {
			require.ErrorContains(t, err, expected.ErrorContains)
		}
	} else {
		require.NoError(t, err)
	}
	if err != nil {
		return
	}

	if expected.Postings == nil {
		expected.Postings = make([]Posting, 0)
	}
	if expected.Metadata == nil {
		expected.Metadata = make(map[string]machine.Value)
	}

	assert.Equalf(t, expected.Postings, m.Postings, "unexpected postings output: %v", m.Postings)
	assert.Equalf(t, expected.Metadata, m.TxMeta, "unexpected metadata output: %v", m.TxMeta)

	wg.Wait()

	assert.Equalf(t, expected.Printed, printed, "unexpected metadata output: %v", printed)
}

func TestFail(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, "fail")
	tc.expected = CaseResult{
		Printed:  []machine.Value{},
		Postings: []Posting{},
		Error:    machine.ErrScriptFailed,
	}
	test(t, tc)
}

func TestPrint(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, "print 29 + 15 - 2")
	mi := machine.MonetaryInt(*big.NewInt(42))
	tc.expected = CaseResult{
		Printed:  []machine.Value{&mi},
		Postings: []Posting{},
		Error:    nil,
	}
	test(t, tc)
}

func TestSend(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [EUR/2 100] (
		source=@alice
		destination=@bob
	)`)
	tc.setBalance("alice", "EUR/2", 100)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      machine.NewMonetaryInt(100),
				Source:      "alice",
				Destination: "bob",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestVariables(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $rider
		account $driver
		string 	$description
 		number 	$nb
 		asset 	$ass
	}
	send [$ass 999] (
		source=$rider
		destination=$driver
	)
 	set_tx_meta("description", $description)
 	set_tx_meta("ride", $nb)`)
	tc.vars = map[string]string{
		"rider":       "users:001",
		"driver":      "users:002",
		"description": "midnight ride",
		"nb":          "1",
		"ass":         "EUR/2",
	}
	tc.setBalance("users:001", "EUR/2", 1000)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      machine.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]machine.Value{
			"description": machine.String("midnight ride"),
			"ride":        machine.NewMonetaryInt(1),
		},
		Error: nil,
	}
}

func TestVariablesJSON(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $rider
		account $driver
		string 	$description
		number 	$nb
		asset 	$ass
	}
	send [$ass 999] (
		source=$rider
		destination=$driver
	)
	set_tx_meta("description", $description)
	set_tx_meta("ride", $nb)`)
	tc.setVarsFromJSON(t, `{
		"rider": "users:001",
		"driver": "users:002",
		"description": "midnight ride",
		"nb": "1",
 		"ass": "EUR/2"
	}`)
	tc.setBalance("users:001", "EUR/2", 1000)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      machine.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]machine.Value{
			"description": machine.String("midnight ride"),
			"ride":        machine.NewMonetaryInt(1),
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSource(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $balance
		account $payment
		account $seller
	}
	send [GEM 15] (
		source = {
			$balance
			$payment
		}
		destination = $seller
	)`)
	tc.setVarsFromJSON(t, `{
		"balance": "users:001",
		"payment": "payments:001",
		"seller": "users:002"
	}`)
	tc.setBalance("users:001", "GEM", 3)
	tc.setBalance("payments:001", "GEM", 12)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(3),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(12),
				Source:      "payments:001",
				Destination: "users:002",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestAllocation(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $rider
		account $driver
	}
	send [GEM 15] (
		source = $rider
		destination = {
			80% to $driver
			8% to @a
			12% to @b
		}
	)`)
	tc.setVarsFromJSON(t, `{
		"rider": "users:001",
		"driver": "users:002"
	}`)
	tc.setBalance("users:001", "GEM", 15)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(13),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(1),
				Source:      "users:001",
				Destination: "a",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(1),
				Source:      "users:001",
				Destination: "b",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestDynamicAllocation(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		portion $p
	}
	send [GEM 15] (
		source = @a
		destination = {
			80% to @b
			$p to @c
			remaining to @d
		}
	)`)
	tc.setVarsFromJSON(t, `{
		"p": "15%"
	}`)
	tc.setBalance("a", "GEM", 15)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(13),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(2),
				Source:      "a",
				Destination: "c",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSendAll(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [USD/2 *] (
		source = @users:001
		destination = @platform
	)`)
	tc.setBalance("users:001", "USD/2", 17)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      machine.NewMonetaryInt(17),
				Source:      "users:001",
				Destination: "platform",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSendAllMulti(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [USD/2 *] (
		source = {
		  @users:001:wallet
		  @users:001:credit
		}
		destination = @platform
	)
	`)
	tc.setBalance("users:001:wallet", "USD/2", 19)
	tc.setBalance("users:001:credit", "USD/2", 22)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      machine.NewMonetaryInt(19),
				Source:      "users:001:wallet",
				Destination: "platform",
			},
			{
				Asset:       "USD/2",
				Amount:      machine.NewMonetaryInt(22),
				Source:      "users:001:credit",
				Destination: "platform",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestInsufficientFunds(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $balance
		account $payment
		account $seller
	}
	send [GEM 16] (
		source = {
			$balance
			$payment
		}
		destination = $seller
	)`)
	tc.setVarsFromJSON(t, `{
		"balance": "users:001",
		"payment": "payments:001",
		"seller": "users:002"
	}`)
	tc.setBalance("users:001", "GEM", 3)
	tc.setBalance("payments:001", "GEM", 12)
	tc.expected = CaseResult{
		Printed:  []machine.Value{},
		Postings: []Posting{},
		Error:    &machine.ErrInsufficientFund{},
	}
	test(t, tc)
}

func TestWorldSource(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 15] (
		source = {
			@a
			@world
		}
		destination = @b
	)`)
	tc.setBalance("a", "GEM", 1)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(1),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(14),
				Source:      "world",
				Destination: "b",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestNoEmptyPostings(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 2] (
		source = @world
		destination = {
			90% to @a
			10% to @b
		}
	)`)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(2),
				Source:      "world",
				Destination: "a",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestEmptyPostings(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM *] (
		source = @foo
		destination = @bar
	)`)
	tc.setBalance("foo", "GEM", 0)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Source:      "foo",
				Destination: "bar",
				Amount:      machine.NewMonetaryInt(0),
				Asset:       "GEM",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestAllocateDontTakeTooMuch(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [CREDIT 200] (
		source = {
			@users:001
			@users:002
		}
		destination = {
			1/2 to @foo
			1/2 to @bar
		}
	)`)
	tc.setBalance("users:001", "CREDIT", 100)
	tc.setBalance("users:002", "CREDIT", 110)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "CREDIT",
				Amount:      machine.NewMonetaryInt(100),
				Source:      "users:001",
				Destination: "foo",
			},
			{
				Asset:       "CREDIT",
				Amount:      machine.NewMonetaryInt(100),
				Source:      "users:002",
				Destination: "bar",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestMetadata(t *testing.T) {
	//commission, _ := machine.NewPortionSpecific(*big.NewRat(125, 1000))
	tc := NewTestCase()
	tc.compile(t, `vars {
		account $sale
		account $seller = meta($sale, "seller")
		portion $commission = meta($seller, "commission")
	}
	send [EUR/2 100] (
		source = $sale
		destination = {
			remaining to $seller
			$commission to @platform
		}
	)`)
	tc.setVarsFromJSON(t, `{
		"sale": "sales:042"
	}`)
	tc.meta = map[string]metadata.Metadata{
		"sales:042": {
			"seller": "users:053",
		},
		"users:053": {
			"commission": "12.5%",
		},
	}
	tc.setBalance("sales:042", "EUR/2", 2500)
	tc.setBalance("users:053", "EUR/2", 500)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      machine.NewMonetaryInt(88),
				Source:      "sales:042",
				Destination: "users:053",
			},
			{
				Asset:       "EUR/2",
				Amount:      machine.NewMonetaryInt(12),
				Source:      "sales:042",
				Destination: "platform",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestTrackBalances(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `
	send [COIN 50] (
		source = @world
		destination = @a
	)
	send [COIN 100] (
		source = @a
		destination = @b
	)`)
	tc.setBalance("a", "COIN", 50)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(50),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(100),
				Source:      "a",
				Destination: "b",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestTrackBalances2(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `
	send [COIN 50] (
		source = @a
		destination = @z
	)
	send [COIN 50] (
		source = @a
		destination = @z
	)`)
	tc.setBalance("a", "COIN", 60)
	tc.expected = CaseResult{
		Printed:  []machine.Value{},
		Postings: []Posting{},
		Error:    &machine.ErrInsufficientFund{},
	}
	test(t, tc)
}

func TestTrackBalances3(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [COIN *] (
		source = @foo
		destination = {
			max [COIN 1000] to @bar
			remaining kept
		}
	)
	send [COIN *] (
		source = @foo
		destination = @bar
	)`)
	tc.setBalance("foo", "COIN", 2000)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "bar",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "bar",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSourceAllotment(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [COIN 100] (
		source = {
			60% from @a
			35.5% from @b
			4.5% from @c
		}
		destination = @d
	)`)
	tc.setBalance("a", "COIN", 100)
	tc.setBalance("b", "COIN", 100)
	tc.setBalance("c", "COIN", 100)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(61),
				Source:      "a",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(35),
				Source:      "b",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(4),
				Source:      "c",
				Destination: "d",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSourceOverlapping(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [COIN 99] (
		source = {
			15% from {
				@b
				@a
			}
			30% from @a
			remaining from @a
		}
		destination = @world
	)`)
	tc.setBalance("a", "COIN", 99)
	tc.setBalance("b", "COIN", 3)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(3),
				Source:      "b",
				Destination: "world",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(96),
				Source:      "a",
				Destination: "world",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSourceComplex(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		monetary $max
	}
	send [COIN 200] (
		source = {
			50% from {
				max [COIN 4] from @a
				@b
				@c
			}
			remaining from max $max from @d
		}
		destination = @platform
	)`)
	tc.setVarsFromJSON(t, `{
		"max": "COIN 120"
	}`)
	tc.setBalance("a", "COIN", 1000)
	tc.setBalance("b", "COIN", 40)
	tc.setBalance("c", "COIN", 1000)
	tc.setBalance("d", "COIN", 1000)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(4),
				Source:      "a",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(40),
				Source:      "b",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(56),
				Source:      "c",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(100),
				Source:      "d",
				Destination: "platform",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestDestinationComplex(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [COIN 100] (
		source = @world
		destination = {
			20% to @a
			20% kept
			60% to {
				max [COIN 10] to @b
				remaining to @c
			}
		}
	)`)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(20),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(10),
				Source:      "world",
				Destination: "b",
			},
			{
				Asset:       "COIN",
				Amount:      machine.NewMonetaryInt(50),
				Source:      "world",
				Destination: "c",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestNeededBalances(t *testing.T) {
	p, err := compiler.Compile(`vars {
		account $a
	}
	send [GEM 15] (
		source = {
			$a
			@b
			@world
		}
		destination = @c
	)`)

	if err != nil {
		t.Fatalf("did not expect error on Compile, got: %v", err)
	}

	m := NewMachine(*p)

	err = m.SetVarsFromJSON(map[string]string{
		"a": "a",
	})
	if err != nil {
		t.Fatalf("did not expect error on SetVars, got: %v", err)
	}
	_, _, err = m.ResolveResources(context.Background(), EmptyStore)
	require.NoError(t, err)

	err = m.ResolveBalances(context.Background(), EmptyStore)
	require.NoError(t, err)
}

func TestSetTxMeta(t *testing.T) {
	p, err := compiler.Compile(`
	set_tx_meta("aaa", @platform)
	set_tx_meta("bbb", GEM)
	set_tx_meta("ccc", 45)
	set_tx_meta("ddd", "hello")
	set_tx_meta("eee", [COIN 30])
	set_tx_meta("fff", 15%)
	`)
	require.NoError(t, err)

	m := NewMachine(*p)

	_, _, err = m.ResolveResources(context.Background(), EmptyStore)
	require.NoError(t, err)
	err = m.ResolveBalances(context.Background(), EmptyStore)
	require.NoError(t, err)

	err = m.Execute()
	require.NoError(t, err)

	expectedMeta := map[string]string{
		"aaa": "platform",
		"bbb": "GEM",
		"ccc": "45",
		"ddd": "hello",
		"eee": "COIN 30",
		"fff": "3/20",
	}

	resMeta := m.GetTxMetaJSON()
	assert.Equal(t, 6, len(resMeta))

	for key, val := range resMeta {
		assert.Equal(t, string(expectedMeta[key]), val)
	}
}

func TestSetAccountMeta(t *testing.T) {
	t.Run("all types", func(t *testing.T) {
		p, err := compiler.Compile(`
			set_account_meta(@platform, "aaa", @platform)
			set_account_meta(@platform, "bbb", GEM)
			set_account_meta(@platform, "ccc", 45)
			set_account_meta(@platform, "ddd", "hello")
			set_account_meta(@platform, "eee", [COIN 30])
			set_account_meta(@platform, "fff", 15%)`)
		require.NoError(t, err)

		m := NewMachine(*p)

		_, _, err = m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.NoError(t, err)

		expectedMeta := metadata.Metadata{
			"aaa": "platform",
			"bbb": "GEM",
			"ccc": "45",
			"ddd": "hello",
			"eee": "COIN 30",
			"fff": "3/20",
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "platform", acc)
			assert.Equal(t, 6, len(meta))
			for key, val := range meta {
				assert.Equal(t, expectedMeta[key], val)
			}
		}
	})

	t.Run("with vars", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				account $acc
			}
			send [EUR/2 100] (
				source = @world
				destination = $acc
			)
			set_account_meta($acc, "fees", 1%)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"acc": "test",
		}))

		_, _, err = m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.NoError(t, err)

		expectedMeta := map[string]json.RawMessage{
			"fees": json.RawMessage("1/100"),
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "test", acc)
			assert.Equal(t, 1, len(meta))
			for key, val := range meta {
				assert.Equal(t, string(expectedMeta[key]), val)
			}
		}
	})
}

func TestVariableBalance(t *testing.T) {
	script := `
		vars {
		  monetary $initial = balance(@A, USD/2)
		}
		send [USD/2 100] (
		  source = {
			@A
			@C
		  }
		  destination = {
			max $initial to @B
			remaining to @D
		  }
		)`

	t.Run("1", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD/2", 40)
		tc.setBalance("C", "USD/2", 90)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(60),
					Source:      "C",
					Destination: "D",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("2", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD/2", 400)
		tc.setBalance("C", "USD/2", 90)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(100),
					Source:      "A",
					Destination: "B",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	script = `
		vars {
		  account $acc
		  monetary $initial = balance($acc, USD/2)
		}
		send [USD/2 100] (
		  source = {
			$acc
			@C
		  }
		  destination = {
			max $initial to @B
			remaining to @D
		  }
		)`

	t.Run("3", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD/2", 40)
		tc.setBalance("C", "USD/2", 90)
		tc.setVarsFromJSON(t, `{"acc": "A"}`)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(60),
					Source:      "C",
					Destination: "D",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("4", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD/2", 400)
		tc.setBalance("C", "USD/2", 90)
		tc.setVarsFromJSON(t, `{"acc": "A"}`)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      machine.NewMonetaryInt(100),
					Source:      "A",
					Destination: "B",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("5", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, `
		vars {
			monetary $max = balance(@maxAcc, COIN)
		}
		send [COIN 200] (
			source = {
				50% from {
					max [COIN 4] from @a
					@b
					@c
				}
				remaining from max $max from @d
			}
			destination = @platform
		)`)
		tc.setBalance("maxAcc", "COIN", 120)
		tc.setBalance("a", "COIN", 1000)
		tc.setBalance("b", "COIN", 40)
		tc.setBalance("c", "COIN", 1000)
		tc.setBalance("d", "COIN", 1000)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "COIN",
					Amount:      machine.NewMonetaryInt(4),
					Source:      "a",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      machine.NewMonetaryInt(40),
					Source:      "b",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      machine.NewMonetaryInt(56),
					Source:      "c",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      machine.NewMonetaryInt(100),
					Source:      "d",
					Destination: "platform",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("send negative monetary", func(t *testing.T) {
		tc := NewTestCase()
		script = `
		vars {
		  monetary $amount = balance(@world, USD/2)
		}
		send $amount (
		  source = @A
		  destination = @B
		)`
		tc.compile(t, script)
		tc.setBalance("world", "USD/2", -40)
		tc.expected = CaseResult{
			Error:         &machine.ErrNegativeAmount{},
			ErrorContains: "must be non-negative",
		}
		test(t, tc)
	})
}

func TestVariablesParsing(t *testing.T) {
	t.Run("account", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				account $acc
			}
			set_tx_meta("account", $acc)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"acc": "valid:acc",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"acc": "invalid-acc",
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"acc": "valid:acc",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"acc": "invalid-acc",
		}))
	})

	t.Run("asset", func(t *testing.T) {
		p, err := compiler.Compile(`
 			vars {
 				asset $ass
 			}
 			set_tx_meta("asset", $ass)
 		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"ass": "USD/2",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"ass": "USD-2",
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"ass": "USD/2",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"ass": "USD-2",
		}))
	})

	t.Run("monetary", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				monetary $mon
			}
			set_tx_meta("monetary", $mon)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"mon": "EUR/2 100",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"mon": "invalid-asset 100",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"mon": "EUR/2",
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"mon": "EUR/2 100",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"mon": "invalid-asset 100",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"mon": "EUR/2 null",
		}))
	})

	t.Run("portion", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				portion $por
			}
			set_tx_meta("portion", $por)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"por": "1/2",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"por": "",
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"por": "1/2",
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"por": "50%",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"por": "3/2",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"por": "200%",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"por": "",
		}))
	})

	t.Run("string", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				string $str
			}
			set_tx_meta("string", $str)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)
		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"str": "valid string",
		}))
	})

	t.Run("number", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				number $nbr
			}
			set_tx_meta("number", $nbr)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVarsFromJSON(map[string]string{
			"nbr": "100",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"nbr": "string",
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]string{
			"nbr": `nil`,
		}))
	})

	t.Run("missing variable", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				number $nbr
				string $str
			}
			set_tx_meta("number", $nbr)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.ErrorContains(t, m.SetVarsFromJSON(map[string]string{
			"nbr": "100",
		}), "missing variable $str")
	})

	t.Run("extraneous variable SetVars", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				number $nbr
			}
			set_tx_meta("number", $nbr)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.ErrorContains(t, m.SetVarsFromJSON(map[string]string{
			"nbr":  "100",
			"nbr2": "100",
		}), "extraneous variable $nbr2")
	})

	t.Run("extraneous variable SetVarsFromJSON", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				number $nbr
			}
			set_tx_meta("number", $nbr)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.ErrorContains(t, m.SetVarsFromJSON(map[string]string{
			"nbr":  `100`,
			"nbr2": `100`,
		}), "extraneous variable $nbr2")
	})
}

func TestVariablesErrors(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
		monetary $mon
	}
	send $mon (
		source = @alice
		destination = @bob
	)`)
	tc.setBalance("alice", "COIN", 10)
	tc.vars = map[string]string{
		"mon": "COIN -1",
	}
	tc.expected = CaseResult{
		Printed:       []machine.Value{},
		Postings:      []Posting{},
		Error:         &machine.ErrInvalidVars{},
		ErrorContains: "negative amount",
	}
	test(t, tc)
}

func TestSetVarsFromJSON(t *testing.T) {

	type testCase struct {
		name          string
		script        string
		expectedError error
		vars          map[string]string
	}
	for _, tc := range []testCase{
		{
			name: "missing var",
			script: `vars {
				account $dest
			}
			send [COIN 99] (
				source = @world
				destination = $dest
			)`,
			expectedError: fmt.Errorf("missing variable $dest"),
		},
		{
			name: "invalid format for account",
			script: `vars {
				account $dest
			}
			send [COIN 99] (
				source = @world
				destination = $dest
			)`,
			vars: map[string]string{
				"dest": "invalid-acc",
			},
			expectedError: fmt.Errorf("invalid JSON value for variable $dest of type account: value invalid-acc: accounts should respect pattern ^[a-zA-Z0-9_]+(:[a-zA-Z0-9_]+)*$"),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			m := NewMachine(*p)
			err = m.SetVarsFromJSON(tc.vars)
			if tc.expectedError != nil {
				require.Error(t, err)
				//TODO(gfyrag): refine error handling of SetVars/ResolveResources/ResolveBalances
				require.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.Nil(t, err)
			}
		})
	}
}

func TestResolveResources(t *testing.T) {

	type testCase struct {
		name          string
		script        string
		expectedError error
		vars          map[string]string
	}
	for _, tc := range []testCase{
		{
			name: "missing metadata",
			script: `vars {
				account $sale
				account $seller = meta($sale, "seller")
			}
			send [COIN *] (
				source = $sale
				destination = $seller
			)`,
			vars: map[string]string{
				"sale": "sales:042",
			},
			expectedError: &machine.ErrMissingMetadata{},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			m := NewMachine(*p)
			require.NoError(t, m.SetVarsFromJSON(tc.vars))
			_, _, err = m.ResolveResources(context.Background(), EmptyStore)
			if tc.expectedError != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expectedError))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResolveBalances(t *testing.T) {

	type testCase struct {
		name          string
		script        string
		expectedError error
		vars          map[string]string
		store         Store
	}
	for _, tc := range []testCase{
		{
			name: "balance function with negative balance",
			store: StaticStore{
				"users:001": &AccountWithBalances{
					Account: ledger.Account{
						Address:  "users:001",
						Metadata: metadata.Metadata{},
					},
					Balances: map[string]*big.Int{
						"COIN": big.NewInt(-100),
					},
				},
			},
			script: `
				vars {
					monetary $bal = balance(@users:001, COIN)
				}
				send $bal (
					source = @users:001
					destination = @world
				)`,
			expectedError: &machine.ErrNegativeAmount{},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := compiler.Compile(tc.script)
			require.NoError(t, err)

			m := NewMachine(*p)
			require.NoError(t, m.SetVarsFromJSON(tc.vars))
			_, _, err = m.ResolveResources(context.Background(), EmptyStore)
			require.NoError(t, err)

			store := tc.store
			if store == nil {
				store = EmptyStore
			}

			err = m.ResolveBalances(context.Background(), store)
			if tc.expectedError != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expectedError))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMachine(t *testing.T) {
	p, err := compiler.Compile(`
		vars {
			account $dest
		}
		send [COIN 99] (
			source = @world
			destination = $dest
		)`)
	require.NoError(t, err)

	t.Run("with debug", func(t *testing.T) {
		m := NewMachine(*p)
		m.Debug = true

		err = m.SetVarsFromJSON(map[string]string{
			"dest": "charlie",
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.NoError(t, err)
	})

	t.Run("err resources", func(t *testing.T) {
		m := NewMachine(*p)
		err := m.Execute()
		require.True(t, errors.Is(err, machine.ErrResourcesNotInitialized))
	})

	t.Run("err balances not initialized", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVarsFromJSON(map[string]string{
			"dest": "charlie",
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.True(t, errors.Is(err, machine.ErrBalancesNotInitialized))
	})

	t.Run("err resolve resources twice", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVarsFromJSON(map[string]string{
			"dest": "charlie",
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		_, _, err = m.ResolveResources(context.Background(), EmptyStore)
		require.ErrorContains(t, err, "tried to call ResolveResources twice")
	})

	t.Run("err missing var", func(t *testing.T) {
		m := NewMachine(*p)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.Error(t, err)
	})
}

func TestVariableAsset(t *testing.T) {
	script := `
 		vars {
 			asset $ass
 			monetary $bal = balance(@alice, $ass)
 		}

 		send [$ass 15] (
 			source = {
 				@alice
 				@bob
 			}
 			destination = @swap
 		)

 		send [$ass *] (
 			source = @swap
 			destination = {
 				max $bal to @alice_2
 				remaining to @bob_2
 			}
 		)`

	tc := NewTestCase()
	tc.compile(t, script)
	tc.vars = map[string]string{
		"ass": "USD",
	}
	tc.setBalance("alice", "USD", 10)
	tc.setBalance("bob", "USD", 10)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "USD",
				Amount:      machine.NewMonetaryInt(10),
				Source:      "alice",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      machine.NewMonetaryInt(5),
				Source:      "bob",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      machine.NewMonetaryInt(10),
				Source:      "swap",
				Destination: "alice_2",
			},
			{
				Asset:       "USD",
				Amount:      machine.NewMonetaryInt(5),
				Source:      "swap",
				Destination: "bob_2",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestSaveFromAccount(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		script := `
 			save [USD 10] from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(20),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("save all", func(t *testing.T) {
		script := `
 			save [USD *] from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(0),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(30),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("save more than balance", func(t *testing.T) {
		script := `
 			save [USD 30] from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(0),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(30),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("with asset var", func(t *testing.T) {
		script := `
			vars {
				asset $ass
			}
 			save [$ass 10] from @alice

 			send [$ass 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.vars = map[string]string{
			"ass": "USD",
		}
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(20),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("with monetary var", func(t *testing.T) {
		script := `
			vars {
				monetary $mon
			}

 			save $mon from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.vars = map[string]string{
			"mon": "USD 10",
		}
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(20),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("multi postings", func(t *testing.T) {
		script := `
 			send [USD 10] (
 			   source = @alice
 			   destination = @bob
 			)

			save [USD 5] from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(5),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(25),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("save a different asset", func(t *testing.T) {
		script := `
			save [COIN 100] from @alice

 			send [USD 30] (
 			   source = {
 				  @alice
 				  @world
 			   }
 			   destination = @bob
 			)`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("alice", "COIN", 100)
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []machine.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(20),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      machine.NewMonetaryInt(10),
					Source:      "world",
					Destination: "bob",
				},
			},
			Error: nil,
		}
		test(t, tc)
	})

	t.Run("negative amount", func(t *testing.T) {
		script := `
			vars {
			  monetary $amt = balance(@A, USD)
			}
			save $amt from @A`
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD", -100)
		tc.expected = CaseResult{
			Printed:  []machine.Value{},
			Postings: []Posting{},
			Error:    &machine.ErrNegativeAmount{},
		}
		test(t, tc)
	})
}

func TestUseDifferentAssetsWithSameSourceAccount(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `vars {
	account $a_account
}
send [A 100] (
	source = $a_account allowing unbounded overdraft
	destination = @account1
)
send [B 100] (
	source = @world
	destination = @account2
)`)
	tc.setBalance("account1", "A", 100)
	tc.setBalance("account2", "B", 100)
	tc.setVarsFromJSON(t, `{"a_account": "world"}`)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{{
			Source:      "world",
			Destination: "account1",
			Amount:      machine.NewMonetaryInt(100),
			Asset:       "A",
		}, {
			Source:      "world",
			Destination: "account2",
			Amount:      machine.NewMonetaryInt(100),
			Asset:       "B",
		}},
	}
	test(t, tc)
}
