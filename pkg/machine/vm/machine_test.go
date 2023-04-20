package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/internal"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	DEBUG bool = false
)

type CaseResult struct {
	Printed       []internal.Value
	Postings      []Posting
	Metadata      map[string]internal.Value
	Error         error
	ErrorContains string
}

type TestCase struct {
	program  *program.Program
	vars     map[string]internal.Value
	meta     map[string]map[string]internal.Value
	balances map[string]map[string]*internal.MonetaryInt
	expected CaseResult
}

func NewTestCase() TestCase {
	return TestCase{
		vars:     make(map[string]internal.Value),
		meta:     make(map[string]map[string]internal.Value),
		balances: make(map[string]map[string]*internal.MonetaryInt),
		expected: CaseResult{
			Printed:  []internal.Value{},
			Postings: []Posting{},
			Metadata: make(map[string]internal.Value),
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
	var jsonVars map[string]json.RawMessage
	err := json.Unmarshal([]byte(str), &jsonVars)
	require.NoError(t, err)
	v, err := c.program.ParseVariablesJSON(jsonVars)
	require.NoError(t, err)
	c.vars = v
}

func (c *TestCase) setBalance(account, asset string, amount int64) {
	if _, ok := c.balances[account]; !ok {
		c.balances[account] = make(map[string]*internal.MonetaryInt)
	}
	c.balances[account][asset] = internal.NewMonetaryInt(amount)
}

func test(t *testing.T, testCase TestCase) {
	testImpl(t, testCase.program, testCase.expected, func(m *Machine) error {
		if err := m.SetVars(testCase.vars); err != nil {
			return err
		}

		store := StoreFn(func(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
			m := metadata.Metadata{}
			for s, value := range testCase.meta[address] {
				json, err := internal.NewJSONFromValue(value)
				require.NoError(t, err)

				m[s] = fmt.Sprintf(`{"type": "%s", "value": "%s"}`, value.GetType().String(), json)
			}
			return &core.AccountWithVolumes{
				Account: core.Account{
					Address:  address,
					Metadata: m,
				},
				Volumes: func() core.AssetsVolumes {
					ret := make(core.AssetsVolumes, 0)
					for asset, balance := range testCase.balances[address] {
						if balance.Gt(internal.NewMonetaryInt(0)) {
							ret[asset] = core.NewEmptyVolumes().WithInput((*big.Int)(balance))
						} else {
							ret[asset] = core.NewEmptyVolumes().WithOutput((*big.Int)(balance.Neg()))
						}
					}
					return ret
				}(),
			}, nil
		})

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
	printed := []internal.Value{}

	var wg sync.WaitGroup
	wg.Add(1)

	require.NotNil(t, prog)

	m := NewMachine(*prog)
	m.Debug = DEBUG
	m.Printer = func(c chan internal.Value) {
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
		expected.Metadata = make(map[string]internal.Value)
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
		Printed:  []internal.Value{},
		Postings: []Posting{},
		Error:    ErrScriptFailed,
	}
	test(t, tc)
}

func TestPrint(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, "print 29 + 15 - 2")
	mi := internal.MonetaryInt(*big.NewInt(42))
	tc.expected = CaseResult{
		Printed:  []internal.Value{&mi},
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      internal.NewMonetaryInt(100),
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
	tc.vars = map[string]internal.Value{
		"rider":       internal.AccountAddress("users:001"),
		"driver":      internal.AccountAddress("users:002"),
		"description": internal.String("midnight ride"),
		"nb":          internal.NewMonetaryInt(1),
		"ass":         internal.Asset("EUR/2"),
	}
	tc.setBalance("users:001", "EUR/2", 1000)
	tc.expected = CaseResult{
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      internal.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]internal.Value{
			"description": internal.String("midnight ride"),
			"ride":        internal.NewMonetaryInt(1),
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
		"nb": 1,
 		"ass": "EUR/2"
	}`)
	tc.setBalance("users:001", "EUR/2", 1000)
	tc.expected = CaseResult{
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      internal.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]internal.Value{
			"description": internal.String("midnight ride"),
			"ride":        internal.NewMonetaryInt(1),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(3),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(12),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(13),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(1),
				Source:      "users:001",
				Destination: "a",
			},
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(1),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(13),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(2),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      internal.NewMonetaryInt(17),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      internal.NewMonetaryInt(19),
				Source:      "users:001:wallet",
				Destination: "platform",
			},
			{
				Asset:       "USD/2",
				Amount:      internal.NewMonetaryInt(22),
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
		Printed:  []internal.Value{},
		Postings: []Posting{},
		Error:    ErrInsufficientFund,
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(1),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(14),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal.NewMonetaryInt(2),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Source:      "foo",
				Destination: "bar",
				Amount:      internal.NewMonetaryInt(0),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "CREDIT",
				Amount:      internal.NewMonetaryInt(100),
				Source:      "users:001",
				Destination: "foo",
			},
			{
				Asset:       "CREDIT",
				Amount:      internal.NewMonetaryInt(100),
				Source:      "users:002",
				Destination: "bar",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestMetadata(t *testing.T) {
	commission, _ := internal.NewPortionSpecific(*big.NewRat(125, 1000))
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
	tc.meta = map[string]map[string]internal.Value{
		"sales:042": {
			"seller": internal.AccountAddress("users:053"),
		},
		"users:053": {
			"commission": *commission,
		},
	}
	tc.setBalance("sales:042", "EUR/2", 2500)
	tc.setBalance("users:053", "EUR/2", 500)
	tc.expected = CaseResult{
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      internal.NewMonetaryInt(88),
				Source:      "sales:042",
				Destination: "users:053",
			},
			{
				Asset:       "EUR/2",
				Amount:      internal.NewMonetaryInt(12),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(50),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(100),
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
		Printed:  []internal.Value{},
		Postings: []Posting{},
		Error:    ErrInsufficientFund,
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "bar",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(1000),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(61),
				Source:      "a",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(35),
				Source:      "b",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(4),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(3),
				Source:      "b",
				Destination: "world",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(96),
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
		"max": {
			"asset": "COIN",
			"amount": 120
		}
	}`)
	tc.setBalance("a", "COIN", 1000)
	tc.setBalance("b", "COIN", 40)
	tc.setBalance("c", "COIN", 1000)
	tc.setBalance("d", "COIN", 1000)
	tc.expected = CaseResult{
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(4),
				Source:      "a",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(40),
				Source:      "b",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(56),
				Source:      "c",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(100),
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
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(20),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(10),
				Source:      "world",
				Destination: "b",
			},
			{
				Asset:       "COIN",
				Amount:      internal.NewMonetaryInt(50),
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

	err = m.SetVars(map[string]internal.Value{
		"a": internal.AccountAddress("a"),
	})
	if err != nil {
		t.Fatalf("did not expect error on SetVars, got: %v", err)
	}
	_, _, err = m.ResolveResources(context.Background(), EmptyStore)
	require.NoError(t, err)

	called := make(map[string]*struct{})
	err = m.ResolveBalances(context.Background(), StoreFn(func(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
		called[address] = &struct{}{}
		return &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: metadata.Metadata{},
			},
			Volumes: map[string]core.Volumes{},
		}, nil
	}))
	require.NoError(t, err)
	require.NotNil(t, called["a"])
	require.NotNil(t, called["b"])
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

	expectedMeta := map[string]json.RawMessage{
		"aaa": json.RawMessage(`{"type":"account","value":"platform"}`),
		"bbb": json.RawMessage(`{"type":"asset","value":"GEM"}`),
		"ccc": json.RawMessage(`{"type":"number","value":45}`),
		"ddd": json.RawMessage(`{"type":"string","value":"hello"}`),
		"eee": json.RawMessage(`{"type":"monetary","value":{"asset":"COIN","amount":30}}`),
		"fff": json.RawMessage(`{"type":"portion","value":{"remaining":false,"specific":"3/20"}}`),
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
			"aaa": `{"type":"account","value":"platform"}`,
			"bbb": `{"type":"asset","value":"GEM"}`,
			"ccc": `{"type":"number","value":45}`,
			"ddd": `{"type":"string","value":"hello"}`,
			"eee": `{"type":"monetary","value":{"asset":"COIN","amount":30}}`,
			"fff": `{"type":"portion","value":{"remaining":false,"specific":"3/20"}}`,
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "platform", acc)
			assert.Equal(t, 6, len(meta))
			for key, val := range meta {
				assert.Equal(t, string(expectedMeta[key]), string(val))
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"acc": internal.AccountAddress("test"),
		}))

		_, _, err = m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.NoError(t, err)

		expectedMeta := map[string]json.RawMessage{
			"fees": json.RawMessage(`{"type":"portion","value":{"remaining":false,"specific":"1/100"}}`),
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "test", acc)
			assert.Equal(t, 1, len(meta))
			for key, val := range meta {
				assert.Equal(t, string(expectedMeta[key]), string(val))
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(60),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(100),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(60),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      internal.NewMonetaryInt(100),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "COIN",
					Amount:      internal.NewMonetaryInt(4),
					Source:      "a",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      internal.NewMonetaryInt(40),
					Source:      "b",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      internal.NewMonetaryInt(56),
					Source:      "c",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      internal.NewMonetaryInt(100),
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
			Error:         ErrNegativeMonetaryAmount,
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"acc": internal.AccountAddress("valid:acc"),
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"acc": internal.AccountAddress("invalid-acc"),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"acc": json.RawMessage(`"valid:acc"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"acc": json.RawMessage(`"invalid-acc"`),
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"ass": internal.Asset("USD/2"),
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"ass": internal.Asset("USD-2"),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"ass": json.RawMessage(`"USD/2"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"ass": json.RawMessage(`"USD-2"`),
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"mon": internal.Monetary{
				Asset:  "EUR/2",
				Amount: internal.NewMonetaryInt(100),
			},
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"mon": internal.Monetary{
				Asset:  "invalid-asset",
				Amount: internal.NewMonetaryInt(100),
			},
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"mon": internal.Monetary{
				Asset:  "EUR/2",
				Amount: nil,
			},
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"mon": json.RawMessage(`{"asset":"EUR/2","amount":100}`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"mon": json.RawMessage(`{"asset":"invalid-asset","amount":100}`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"mon": json.RawMessage(`{"asset":"EUR/2","amount":null}`),
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"por": internal.Portion{
				Remaining: false,
				Specific:  big.NewRat(1, 2),
			},
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"por": internal.Portion{
				Remaining: false,
				Specific:  nil,
			},
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"por": internal.Portion{
				Remaining: true,
				Specific:  big.NewRat(1, 2),
			},
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"por": json.RawMessage(`"1/2"`),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"por": json.RawMessage(`"50%"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"por": json.RawMessage(`"3/2"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"por": json.RawMessage(`"200%"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"por": json.RawMessage(`""`),
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"str": internal.String("valid string"),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"str": json.RawMessage(`"valid string"`),
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"str": internal.NewMonetaryInt(1),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"str": json.RawMessage(`100`),
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

		require.NoError(t, m.SetVars(map[string]internal.Value{
			"nbr": internal.NewMonetaryInt(100),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"nbr": json.RawMessage(`100`),
		}))

		require.Error(t, m.SetVars(map[string]internal.Value{
			"nbr": internal.String("invalid"),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"nbr": json.RawMessage(`"string"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"nbr": json.RawMessage(`nil`),
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

		require.ErrorContains(t, m.SetVars(map[string]internal.Value{
			"nbr": internal.NewMonetaryInt(100),
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

		require.ErrorContains(t, m.SetVars(map[string]internal.Value{
			"nbr":  internal.NewMonetaryInt(100),
			"nbr2": internal.NewMonetaryInt(100),
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

		require.ErrorContains(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"nbr":  json.RawMessage(`100`),
			"nbr2": json.RawMessage(`100`),
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
	tc.vars = map[string]internal.Value{
		"mon": internal.Monetary{
			Asset:  "COIN",
			Amount: internal.NewMonetaryInt(-1),
		},
	}
	tc.expected = CaseResult{
		Printed:       []internal.Value{},
		Postings:      []Posting{},
		Error:         ErrInvalidVars,
		ErrorContains: "negative amount",
	}
	test(t, tc)
}

func TestSetVarsFromJSON(t *testing.T) {

	type testCase struct {
		name          string
		script        string
		expectedError error
		vars          map[string]json.RawMessage
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
			vars: map[string]json.RawMessage{
				"dest": json.RawMessage(`"invalid-acc"`),
			},
			expectedError: fmt.Errorf("invalid JSON value for variable $dest of type account: value invalid-acc: accounts should respect pattern ^[a-zA-Z_]+[a-zA-Z0-9_:]*$"),
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
		vars          map[string]json.RawMessage
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
			vars: map[string]json.RawMessage{
				"sale": json.RawMessage(`"sales:042"`),
			},
			expectedError: ErrResourceResolutionMissingMetadata,
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
		vars          map[string]json.RawMessage
		store         Store
	}
	for _, tc := range []testCase{
		{
			name: "balance function with negative balance",
			store: StaticStore{
				"users:001": &core.AccountWithVolumes{
					Account: core.Account{
						Address:  "users:001",
						Metadata: metadata.Metadata{},
					},
					Volumes: map[string]core.Volumes{
						"COIN": {
							Input:  big.NewInt(0),
							Output: big.NewInt(100),
						},
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
			expectedError: ErrNegativeMonetaryAmount,
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

		err = m.SetVars(map[string]internal.Value{
			"dest": internal.AccountAddress("charlie"),
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
		require.True(t, errors.Is(err, ErrResourcesNotInitialized))
	})

	t.Run("err balances not initialized", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]internal.Value{
			"dest": internal.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.Execute()
		require.True(t, errors.Is(err, ErrBalancesNotInitialized))
	})

	t.Run("err resolve resources twice", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]internal.Value{
			"dest": internal.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		_, _, err = m.ResolveResources(context.Background(), EmptyStore)
		require.ErrorContains(t, err, "tried to call ResolveResources twice")
	})

	t.Run("err balances before resources", func(t *testing.T) {
		m := NewMachine(*p)

		err := m.ResolveBalances(context.Background(), EmptyStore)
		require.ErrorContains(t, err, "tried to resolve balances before resources")
	})

	t.Run("err resolve balances twice", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]internal.Value{
			"dest": internal.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		_, _, err := m.ResolveResources(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.NoError(t, err)

		err = m.ResolveBalances(context.Background(), EmptyStore)
		require.ErrorContains(t, err, "tried to call ResolveBalances twice")
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
	tc.vars = map[string]internal.Value{
		"ass": internal.Asset("USD"),
	}
	tc.setBalance("alice", "USD", 10)
	tc.setBalance("bob", "USD", 10)
	tc.expected = CaseResult{
		Printed: []internal.Value{},
		Postings: []Posting{
			{
				Asset:       "USD",
				Amount:      internal.NewMonetaryInt(10),
				Source:      "alice",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      internal.NewMonetaryInt(5),
				Source:      "bob",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      internal.NewMonetaryInt(10),
				Source:      "swap",
				Destination: "alice_2",
			},
			{
				Asset:       "USD",
				Amount:      internal.NewMonetaryInt(5),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(20),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(0),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(30),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(0),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(30),
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
		tc.vars = map[string]internal.Value{
			"ass": internal.Asset("USD"),
		}
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(20),
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
		tc.vars = map[string]internal.Value{
			"mon": internal.Monetary{
				Asset:  "USD",
				Amount: internal.NewMonetaryInt(10),
			},
		}
		tc.setBalance("alice", "USD", 20)
		tc.expected = CaseResult{
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(20),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(10),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(5),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(25),
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
			Printed: []internal.Value{},
			Postings: []Posting{
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(20),
					Source:      "alice",
					Destination: "bob",
				},
				{
					Asset:       "USD",
					Amount:      internal.NewMonetaryInt(10),
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
			Printed:  []internal.Value{},
			Postings: []Posting{},
			Error:    ErrNegativeMonetaryAmount,
		}
		test(t, tc)
	})
}

func TestMachine_Execute(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    byte
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			err := m.Execute()
			if !tt.wantErr(t, err, "Execute()") {
				return
			}
		})
	}
}

func TestMachine_GetAccountsMetaJSON(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	tests := []struct {
		name   string
		fields fields
		want   Metadata
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			assert.Equalf(t, tt.want, m.GetAccountsMetaJSON(), "GetAccountsMetaJSON()")
		})
	}
}

func TestMachine_GetTxMetaJSON(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	tests := []struct {
		name   string
		fields fields
		want   Metadata
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			assert.Equalf(t, tt.want, m.GetTxMetaJSON(), "GetTxMetaJSON()")
		})
	}
}

func TestMachine_ResolveBalances(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		ctx   context.Context
		store Store
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			tt.wantErr(t, m.ResolveBalances(tt.args.ctx, tt.args.store), fmt.Sprintf("ResolveBalances(%v, %v)", tt.args.ctx, tt.args.store))
		})
	}
}

func TestMachine_ResolveResources(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		ctx   context.Context
		store Store
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			_, _, err := m.ResolveResources(tt.args.ctx, tt.args.store)
			tt.wantErr(t, err, fmt.Sprintf("ResolveResources(%v, %v)", tt.args.ctx, tt.args.store))
		})
	}
}

func TestMachine_SetVars(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		vars map[string]internal.Value
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			tt.wantErr(t, m.SetVars(tt.args.vars), fmt.Sprintf("SetVars(%v)", tt.args.vars))
		})
	}
}

func TestMachine_SetVarsFromJSON(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		vars map[string]json.RawMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			tt.wantErr(t, m.SetVarsFromJSON(tt.args.vars), fmt.Sprintf("SetVarsFromJSON(%v)", tt.args.vars))
		})
	}
}

func TestMachine_credit(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		account internal.AccountAddress
		funding internal.Funding
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			m.credit(tt.args.account, tt.args.funding)
		})
	}
}

func TestMachine_getResource(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		addr internal.Address
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *internal.Value
		want1  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			got, got1 := m.getResource(tt.args.addr)
			assert.Equalf(t, tt.want, got, "getResource(%v)", tt.args.addr)
			assert.Equalf(t, tt.want1, got1, "getResource(%v)", tt.args.addr)
		})
	}
}

func TestMachine_popValue(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	tests := []struct {
		name   string
		fields fields
		want   internal.Value
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			assert.Equalf(t, tt.want, m.popValue(), "popValue()")
		})
	}
}

func TestMachine_pushValue(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		v internal.Value
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			m.pushValue(tt.args.v)
		})
	}
}

func TestMachine_repay(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		funding internal.Funding
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			m.repay(tt.args.funding)
		})
	}
}

func TestMachine_tick(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
		want1  byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			got, got1 := m.tick()
			assert.Equalf(t, tt.want, got, "tick()")
			assert.Equalf(t, tt.want1, got1, "tick()")
		})
	}
}

func TestMachine_withdrawAll(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		account   internal.AccountAddress
		asset     internal.Asset
		overdraft *internal.MonetaryInt
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *internal.Funding
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			got, err := m.withdrawAll(tt.args.account, tt.args.asset, tt.args.overdraft)
			if !tt.wantErr(t, err, fmt.Sprintf("withdrawAll(%v, %v, %v)", tt.args.account, tt.args.asset, tt.args.overdraft)) {
				return
			}
			assert.Equalf(t, tt.want, got, "withdrawAll(%v, %v, %v)", tt.args.account, tt.args.asset, tt.args.overdraft)
		})
	}
}

func TestMachine_withdrawAlways(t *testing.T) {
	type fields struct {
		P                   uint
		Program             program.Program
		Vars                map[string]internal.Value
		UnresolvedResources []program.Resource
		Resources           []internal.Value
		resolveCalled       bool
		Balances            map[internal.AccountAddress]map[internal.Asset]*internal.MonetaryInt
		setBalanceCalled    bool
		Stack               []internal.Value
		Postings            []Posting
		TxMeta              map[string]internal.Value
		AccountsMeta        map[internal.AccountAddress]map[string]internal.Value
		Printer             func(chan internal.Value)
		printChan           chan internal.Value
		Debug               bool
	}
	type args struct {
		account internal.AccountAddress
		mon     internal.Monetary
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *internal.Funding
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Machine{
				P:                   tt.fields.P,
				Program:             tt.fields.Program,
				Vars:                tt.fields.Vars,
				UnresolvedResources: tt.fields.UnresolvedResources,
				Resources:           tt.fields.Resources,
				resolveCalled:       tt.fields.resolveCalled,
				Balances:            tt.fields.Balances,
				setBalanceCalled:    tt.fields.setBalanceCalled,
				Stack:               tt.fields.Stack,
				Postings:            tt.fields.Postings,
				TxMeta:              tt.fields.TxMeta,
				AccountsMeta:        tt.fields.AccountsMeta,
				Printer:             tt.fields.Printer,
				printChan:           tt.fields.printChan,
				Debug:               tt.fields.Debug,
			}
			got, err := m.withdrawAlways(tt.args.account, tt.args.mon)
			if !tt.wantErr(t, err, fmt.Sprintf("withdrawAlways(%v, %v)", tt.args.account, tt.args.mon)) {
				return
			}
			assert.Equalf(t, tt.want, got, "withdrawAlways(%v, %v)", tt.args.account, tt.args.mon)
		})
	}
}

func TestNewMachine(t *testing.T) {
	type args struct {
		p program.Program
	}
	tests := []struct {
		name string
		args args
		want *Machine
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewMachine(tt.args.p), "NewMachine(%v)", tt.args.p)
		})
	}
}

func TestStdOutPrinter(t *testing.T) {
	type args struct {
		c chan internal.Value
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			StdOutPrinter(tt.args.c)
		})
	}
}

func TestStoreFn_GetAccountWithVolumes(t *testing.T) {
	type args struct {
		ctx     context.Context
		address string
	}
	tests := []struct {
		name    string
		fn      StoreFn
		args    args
		want    *core.AccountWithVolumes
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fn.GetAccountWithVolumes(tt.args.ctx, tt.args.address)
			if !tt.wantErr(t, err, fmt.Sprintf("GetAccountWithVolumes(%v, %v)", tt.args.ctx, tt.args.address)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetAccountWithVolumes(%v, %v)", tt.args.ctx, tt.args.address)
		})
	}
}

func TestSendWithArithmetic(t *testing.T) {
	t.Run("nominal", func(t *testing.T) {
		tc := NewTestCase()
		script := `
			vars {
				asset $ass
				monetary $mon
			}
			send [EUR 1] + $mon + [$ass 3] - [EUR 4] (
				source = @a
				destination = @b
			)`
		tc.compile(t, script)
		tc.setBalance("a", "EUR", 10)
		tc.vars = map[string]core.Value{
			"ass": core.Asset("EUR"),
			"mon": core.Monetary{
				Asset:  "EUR",
				Amount: core.NewMonetaryInt(2),
			},
		}
		tc.expected = CaseResult{
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "EUR",
					Amount:      core.NewMonetaryInt(2),
					Source:      "a",
					Destination: "b",
				},
			},
			ExitCode: EXIT_OK,
		}
		test(t, tc)
	})

	t.Run("error different assets", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, `
			send [USD 2] + [EUR 1] (
				source = @world
				destination = @alice
			)`)
		tc.expected = CaseResult{
			Printed:  []core.Value{},
			Postings: []Posting{},
			ExitCode: EXIT_FAIL_INVALID,
			Error:    "tried to add two monetary with different assets: 'USD' and 'EUR'",
		}
		test(t, tc)
	})

	t.Run("error negative amount", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, `
			send [USD 2] - [USD 3] (
				source = @world
				destination = @alice
			)`)
		tc.expected = CaseResult{
			Printed:  []core.Value{},
			Postings: []Posting{},
			ExitCode: EXIT_FAIL_INVALID,
			Error:    "cannot send a monetary with a negative amount: [USD -1]",
		}
		test(t, tc)
	})

	t.Run("error insufficient funds", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, `
			send [USD 3] - [USD 1] (
				source = @bob
				destination = @alice
			)`)
		tc.setBalance("bob", "USD", 1)
		tc.expected = CaseResult{
			Printed:  []core.Value{},
			Postings: []Posting{},
			ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
		}
		test(t, tc)
	})
}
