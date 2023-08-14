package vm

import (
	"encoding/json"
	"math/big"
	"sync"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/script/compiler"
	"github.com/numary/ledger/pkg/machine/vm/program"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	DEBUG bool = false
)

type CaseResult struct {
	Printed  []core.Value
	Postings []Posting
	Metadata map[string]core.Value
	ExitCode byte
	Error    string
}

type TestCase struct {
	program  *program.Program
	vars     map[string]core.Value
	meta     map[string]map[string]core.Value
	balances map[string]map[string]*core.MonetaryInt
	expected CaseResult
}

func NewTestCase() TestCase {
	return TestCase{
		vars:     make(map[string]core.Value),
		meta:     make(map[string]map[string]core.Value),
		balances: make(map[string]map[string]*core.MonetaryInt),
		expected: CaseResult{
			Printed:  []core.Value{},
			Postings: []Posting{},
			Metadata: make(map[string]core.Value),
			ExitCode: EXIT_OK,
			Error:    "",
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
		c.balances[account] = make(map[string]*core.MonetaryInt)
	}
	c.balances[account][asset] = core.NewMonetaryInt(amount)
}

func test(t *testing.T, testCase TestCase) {
	testImpl(t, testCase.program, testCase.expected, func(m *Machine) (byte, error) {
		if err := m.SetVars(testCase.vars); err != nil {
			return 0, err
		}

		{
			ch, err := m.ResolveResources()
			if err != nil {
				return 0, err
			}
			for req := range ch {
				if req.Key != "" {
					val := testCase.meta[req.Account][req.Key]
					req.Response <- val
				} else if req.Asset != "" {
					val := testCase.balances[req.Account][req.Asset]
					req.Response <- val
				}
				if req.Error != nil {
					return 0, req.Error
				}
			}
		}

		{
			ch, err := m.ResolveBalances()
			if err != nil {
				return 0, err
			}
			for req := range ch {
				val := testCase.balances[req.Account][req.Asset]
				req.Response <- val
				if req.Error != nil {
					return 0, req.Error
				}
			}
		}

		return m.Execute()
	})
}

func testImpl(t *testing.T, prog *program.Program, expected CaseResult, exec func(*Machine) (byte, error)) {
	printed := []core.Value{}

	var wg sync.WaitGroup
	wg.Add(1)

	if prog == nil {
		t.Fatal("no program")
	}

	m := NewMachine(*prog)
	m.Debug = DEBUG
	m.Printer = func(c chan core.Value) {
		for v := range c {
			printed = append(printed, v)
		}
		wg.Done()
	}

	exitCode, err := exec(m)
	require.Equal(t, expected.ExitCode, exitCode, err)
	if expected.Error != "" {
		require.ErrorContains(t, err, expected.Error)
	} else {
		require.NoError(t, err)
	}

	if exitCode != EXIT_OK {
		return
	}

	if expected.Postings == nil {
		expected.Postings = make([]Posting, 0)
	}
	if expected.Metadata == nil {
		expected.Metadata = make(map[string]core.Value)
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
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL,
	}
	test(t, tc)
}

func TestPrint(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, "print 29 + 15 - 2")
	mi := core.MonetaryInt(*big.NewInt(42))
	tc.expected = CaseResult{
		Printed:  []core.Value{&mi},
		Postings: []Posting{},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      core.NewMonetaryInt(100),
				Source:      "alice",
				Destination: "bob",
			},
		},
		ExitCode: EXIT_OK,
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
	tc.vars = map[string]core.Value{
		"rider":       core.AccountAddress("users:001"),
		"driver":      core.AccountAddress("users:002"),
		"description": core.String("midnight ride"),
		"nb":          core.NewMonetaryInt(1),
		"ass":         core.Asset("EUR/2"),
	}
	tc.setBalance("users:001", "EUR/2", 1000)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      core.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]core.Value{
			"description": core.String("midnight ride"),
			"ride":        core.NewMonetaryInt(1),
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      core.NewMonetaryInt(999),
				Source:      "users:001",
				Destination: "users:002",
			},
		},
		Metadata: map[string]core.Value{
			"description": core.String("midnight ride"),
			"ride":        core.NewMonetaryInt(1),
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(3),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(12),
				Source:      "payments:001",
				Destination: "users:002",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(13),
				Source:      "users:001",
				Destination: "users:002",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1),
				Source:      "users:001",
				Destination: "a",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1),
				Source:      "users:001",
				Destination: "b",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(13),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(2),
				Source:      "a",
				Destination: "c",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      core.NewMonetaryInt(17),
				Source:      "users:001",
				Destination: "platform",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "USD/2",
				Amount:      core.NewMonetaryInt(19),
				Source:      "users:001:wallet",
				Destination: "platform",
			},
			{
				Asset:       "USD/2",
				Amount:      core.NewMonetaryInt(22),
				Source:      "users:001:credit",
				Destination: "platform",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1),
				Source:      "a",
				Destination: "b",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(14),
				Source:      "world",
				Destination: "b",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(2),
				Source:      "world",
				Destination: "a",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestNoEmptyPostings2(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM *] (
		source = @foo
		destination = @bar
	)`)
	tc.setBalance("foo", "GEM", 0)
	tc.expected = CaseResult{
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "CREDIT",
				Amount:      core.NewMonetaryInt(100),
				Source:      "users:001",
				Destination: "foo",
			},
			{
				Asset:       "CREDIT",
				Amount:      core.NewMonetaryInt(100),
				Source:      "users:002",
				Destination: "bar",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestMetadata(t *testing.T) {
	commission, _ := core.NewPortionSpecific(*big.NewRat(125, 1000))
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
	tc.meta = map[string]map[string]core.Value{
		"sales:042": {
			"seller": core.AccountAddress("users:053"),
		},
		"users:053": {
			"commission": *commission,
		},
	}
	tc.setBalance("sales:042", "EUR/2", 2500)
	tc.setBalance("users:053", "EUR/2", 500)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "EUR/2",
				Amount:      core.NewMonetaryInt(88),
				Source:      "sales:042",
				Destination: "users:053",
			},
			{
				Asset:       "EUR/2",
				Amount:      core.NewMonetaryInt(12),
				Source:      "sales:042",
				Destination: "platform",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(50),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(100),
				Source:      "a",
				Destination: "b",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "bar",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "bar",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(61),
				Source:      "a",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(35),
				Source:      "b",
				Destination: "d",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(4),
				Source:      "c",
				Destination: "d",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(3),
				Source:      "b",
				Destination: "world",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(96),
				Source:      "a",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(4),
				Source:      "a",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(40),
				Source:      "b",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(56),
				Source:      "c",
				Destination: "platform",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(100),
				Source:      "d",
				Destination: "platform",
			},
		},
		ExitCode: EXIT_OK,
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(20),
				Source:      "world",
				Destination: "a",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(10),
				Source:      "world",
				Destination: "b",
			},
			{
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(50),
				Source:      "world",
				Destination: "c",
			},
		},
		ExitCode: EXIT_OK,
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

	err = m.SetVars(map[string]core.Value{
		"a": core.AccountAddress("a"),
	})
	if err != nil {
		t.Fatalf("did not expect error on SetVars, got: %v", err)
	}
	{
		ch, err := m.ResolveResources()
		if err != nil {
			t.Fatalf("did not expect error on ResolveResources, got: %v", err)
		}
		for range ch {
			t.Fatalf("did not expect to need any metadata")
		}
	}

	expected := map[string]map[string]struct{}{
		"a": {
			"GEM": {},
		},
		"b": {
			"GEM": {},
		},
	}
	{
		ch, err := m.ResolveBalances()
		if err != nil {
			t.Fatalf("did not expect error on ResolveBalances, got: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				t.Fatalf("did not expect error in balance request: %v", req.Error)
			}
			if _, ok := expected[req.Account][req.Asset]; ok {
				delete(expected[req.Account], req.Asset)
				if len(expected[req.Account]) == 0 {
					delete(expected, req.Account)
				}
				req.Response <- core.NewMonetaryInt(0)
			} else {
				t.Fatalf("did not expect to need %v balance of %v", req.Asset, req.Account)
			}
		}
	}
	if len(expected) != 0 {
		t.Fatalf("some balances were not requested: %v", expected)
	}
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

	{
		ch, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch {
			require.NoError(t, req.Error)
		}
	}

	{
		ch, err := m.ResolveBalances()
		require.NoError(t, err)
		for req := range ch {
			require.NoError(t, req.Error)
		}
	}

	exitCode, err := m.Execute()
	require.NoError(t, err)
	require.Equal(t, EXIT_OK, exitCode)

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
		assert.Equal(t, string(expectedMeta[key]), string(val.([]byte)))
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

		{
			ch, err := m.ResolveResources()
			require.NoError(t, err)
			for req := range ch {
				require.NoError(t, req.Error)
			}
		}

		{
			ch, err := m.ResolveBalances()
			require.NoError(t, err)
			for req := range ch {
				require.NoError(t, req.Error)
			}
		}

		exitCode, err := m.Execute()
		require.NoError(t, err)
		require.Equal(t, EXIT_OK, exitCode)

		expectedMeta := map[string]json.RawMessage{
			"aaa": json.RawMessage(`{"type":"account","value":"platform"}`),
			"bbb": json.RawMessage(`{"type":"asset","value":"GEM"}`),
			"ccc": json.RawMessage(`{"type":"number","value":45}`),
			"ddd": json.RawMessage(`{"type":"string","value":"hello"}`),
			"eee": json.RawMessage(`{"type":"monetary","value":{"asset":"COIN","amount":30}}`),
			"fff": json.RawMessage(`{"type":"portion","value":{"remaining":false,"specific":"3/20"}}`),
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "@platform", acc)
			m := meta.(map[string][]byte)
			assert.Equal(t, 6, len(m))
			for key, val := range m {
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"acc": core.AccountAddress("test"),
		}))

		{
			ch, err := m.ResolveResources()
			require.NoError(t, err)
			for req := range ch {
				require.NoError(t, req.Error)
			}
		}

		{
			ch, err := m.ResolveBalances()
			require.NoError(t, err)
			for req := range ch {
				require.NoError(t, req.Error)
			}
		}

		exitCode, err := m.Execute()
		require.NoError(t, err)
		require.Equal(t, EXIT_OK, exitCode)

		expectedMeta := map[string]json.RawMessage{
			"fees": json.RawMessage(`{"type":"portion","value":{"remaining":false,"specific":"1/100"}}`),
		}

		resMeta := m.GetAccountsMetaJSON()
		assert.Equal(t, 1, len(resMeta))

		for acc, meta := range resMeta {
			assert.Equal(t, "@test", acc)
			m := meta.(map[string][]byte)
			assert.Equal(t, 1, len(m))
			for key, val := range m {
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
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(60),
					Source:      "C",
					Destination: "D",
				},
			},
			ExitCode: EXIT_OK,
		}
		test(t, tc)
	})

	t.Run("2", func(t *testing.T) {
		tc := NewTestCase()
		tc.compile(t, script)
		tc.setBalance("A", "USD/2", 400)
		tc.setBalance("C", "USD/2", 90)
		tc.expected = CaseResult{
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(100),
					Source:      "A",
					Destination: "B",
				},
			},
			ExitCode: EXIT_OK,
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
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(40),
					Source:      "A",
					Destination: "B",
				},
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(60),
					Source:      "C",
					Destination: "D",
				},
			},
			ExitCode: EXIT_OK,
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
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "USD/2",
					Amount:      core.NewMonetaryInt(100),
					Source:      "A",
					Destination: "B",
				},
			},
			ExitCode: EXIT_OK,
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
			Printed: []core.Value{},
			Postings: []Posting{
				{
					Asset:       "COIN",
					Amount:      core.NewMonetaryInt(4),
					Source:      "a",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      core.NewMonetaryInt(40),
					Source:      "b",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      core.NewMonetaryInt(56),
					Source:      "c",
					Destination: "platform",
				},
				{
					Asset:       "COIN",
					Amount:      core.NewMonetaryInt(100),
					Source:      "d",
					Destination: "platform",
				},
			},
			ExitCode: EXIT_OK,
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
			Error: "must be non-negative",
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"acc": core.AccountAddress("valid:acc"),
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"acc": core.AccountAddress("invalid-acc"),
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"ass": core.Asset("USD/2"),
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"ass": core.Asset("USD-2"),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"ass": json.RawMessage(`"USD/2"`),
		}))

		require.Error(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"ass": json.RawMessage(`"USD-2"`),
		}))
	})

	// TODO: handle properly in ledger v1.10
	t.Run("account empty string", func(t *testing.T) {
		p, err := compiler.Compile(`
			vars {
				account $acc
			}
			set_tx_meta("account", $acc)
		`)
		require.NoError(t, err)

		m := NewMachine(*p)

		require.NoError(t, m.SetVars(map[string]core.Value{
			"acc": core.AccountAddress(""),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"acc": json.RawMessage(`""`),
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"mon": core.Monetary{
				Asset:  "EUR/2",
				Amount: core.NewMonetaryInt(100),
			},
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"mon": core.Monetary{
				Asset:  "invalid-asset",
				Amount: core.NewMonetaryInt(100),
			},
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"mon": core.Monetary{
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"por": core.Portion{
				Remaining: false,
				Specific:  big.NewRat(1, 2),
			},
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"por": core.Portion{
				Remaining: false,
				Specific:  nil,
			},
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"por": core.Portion{
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"str": core.String("valid string"),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"str": json.RawMessage(`"valid string"`),
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"str": core.NewMonetaryInt(1),
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

		require.NoError(t, m.SetVars(map[string]core.Value{
			"nbr": core.NewMonetaryInt(100),
		}))

		require.NoError(t, m.SetVarsFromJSON(map[string]json.RawMessage{
			"nbr": json.RawMessage(`100`),
		}))

		require.Error(t, m.SetVars(map[string]core.Value{
			"nbr": core.String("invalid"),
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

		require.ErrorContains(t, m.SetVars(map[string]core.Value{
			"nbr": core.NewMonetaryInt(100),
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

		require.ErrorContains(t, m.SetVars(map[string]core.Value{
			"nbr":  core.NewMonetaryInt(100),
			"nbr2": core.NewMonetaryInt(100),
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
	tc.vars = map[string]core.Value{
		"mon": core.Monetary{
			Asset:  "COIN",
			Amount: core.NewMonetaryInt(-1),
		},
	}
	tc.expected = CaseResult{
		Printed:  []core.Value{},
		Postings: []Posting{},
		Error:    "negative amount",
	}
	test(t, tc)
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

		err = m.SetVars(map[string]core.Value{
			"dest": core.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		ch1, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch1 {
			require.NoError(t, req.Error)
		}

		ch2, err := m.ResolveBalances()
		require.NoError(t, err)
		for req := range ch2 {
			require.NoError(t, req.Error)
		}

		exitCode, err := m.Execute()
		require.NoError(t, err)
		require.Equal(t, EXIT_OK, exitCode)
	})

	t.Run("err resources", func(t *testing.T) {
		m := NewMachine(*p)
		exitCode, err := m.Execute()
		require.ErrorContains(t, err, "resources haven't been initialized")
		require.Equal(t, byte(0), exitCode)
	})

	t.Run("err balances nit initialized", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]core.Value{
			"dest": core.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		ch1, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch1 {
			require.NoError(t, req.Error)
		}

		exitCode, err := m.Execute()
		require.ErrorContains(t, err, "balances haven't been initialized")
		require.Equal(t, byte(0), exitCode)
	})

	t.Run("err resolve resources twice", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]core.Value{
			"dest": core.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		ch1, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch1 {
			require.NoError(t, req.Error)
		}

		_, err = m.ResolveResources()
		require.ErrorContains(t, err, "tried to call ResolveResources twice")
	})

	t.Run("err balances before resources", func(t *testing.T) {
		m := NewMachine(*p)

		_, err := m.ResolveBalances()
		require.ErrorContains(t, err, "tried to resolve balances before resources")
	})

	t.Run("err resolve balances twice", func(t *testing.T) {
		m := NewMachine(*p)

		err = m.SetVars(map[string]core.Value{
			"dest": core.AccountAddress("charlie"),
		})
		require.NoError(t, err)

		ch1, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch1 {
			require.NoError(t, req.Error)
		}

		ch2, err := m.ResolveBalances()
		require.NoError(t, err)
		for req := range ch2 {
			require.NoError(t, req.Error)
		}

		_, err = m.ResolveBalances()
		require.ErrorContains(t, err, "tried to call ResolveBalances twice")
	})

	t.Run("err missing var", func(t *testing.T) {
		m := NewMachine(*p)

		ch1, err := m.ResolveResources()
		require.NoError(t, err)
		for req := range ch1 {
			require.ErrorContains(t, req.Error, "missing variable 'dest'")
		}
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
	tc.vars = map[string]core.Value{
		"ass": core.Asset("USD"),
	}
	tc.setBalance("alice", "USD", 10)
	tc.setBalance("bob", "USD", 10)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "USD",
				Amount:      core.NewMonetaryInt(10),
				Source:      "alice",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      core.NewMonetaryInt(5),
				Source:      "bob",
				Destination: "swap",
			},
			{
				Asset:       "USD",
				Amount:      core.NewMonetaryInt(10),
				Source:      "swap",
				Destination: "alice_2",
			},
			{
				Asset:       "USD",
				Amount:      core.NewMonetaryInt(5),
				Source:      "swap",
				Destination: "bob_2",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
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
		Printed: []core.Value{},
		Postings: []Posting{{
			Source:      "world",
			Destination: "account1",
			Amount:      core.NewMonetaryInt(100),
			Asset:       "A",
		}, {
			Source:      "world",
			Destination: "account2",
			Amount:      core.NewMonetaryInt(100),
			Asset:       "B",
		}},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}
