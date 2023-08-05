package vm

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/internal"
	"github.com/numary/ledger/pkg/machine/script/compiler"
	"github.com/numary/ledger/pkg/machine/vm/program"
	"github.com/numary/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// import (
// 	"encoding/json"
// 	"math/big"
// 	"sync"
// 	"testing"

// 	"github.com/numary/ledger/pkg/core"
// 	"github.com/numary/ledger/pkg/machine/script/compiler"
//  "github.com/numary/ledger/pkg/machine/vm/program"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// )

const (
	DEBUG bool = false
)

type CaseResult struct {
	Printed  []internal.Value
	Postings []Posting
	Metadata map[string]internal.Value
	// Error         string
	Error string
}

type TestCase struct {
	program  program.Program
	store    StaticStore
	vars     map[string]string
	expected CaseResult
}

func NewTestCase() TestCase {
	return TestCase{
		vars:  make(map[string]string),
		store: StaticStore{},
		expected: CaseResult{
			Printed:  []internal.Value{},
			Postings: []Posting{},
			Metadata: make(map[string]internal.Value),
			// Error:    "",
		},
	}
}

func (c *TestCase) compile(t *testing.T, code string) {
	p, err := compiler.Compile(code)
	if err != nil {
		t.Fatalf("compile error: %v", err)
		return
	}
	c.program = *p
}

func (c *TestCase) setVarsFromJSON(t *testing.T, str string) {
	var jsonVars map[string]string
	err := json.Unmarshal([]byte(str), &jsonVars)
	require.NoError(t, err)
	c.vars = jsonVars
}

func (c *TestCase) setBalance(account, asset string, amount int64) {
	if _, ok := c.store[account]; !ok {
		c.store[account] = &AccountWithBalances{
			Account: core.Account{
				Address:  account,
				Metadata: map[string]string{},
			},
			Balances: map[string]*big.Int{},
		}
	}
	amt_bigint := big.Int(*internal.NewMonetaryInt(amount))
	c.store[account].Balances[asset] = &amt_bigint
}

func (c *TestCase) setMeta(account, key string, value string) {
	if _, ok := c.store[account]; !ok {
		c.store[account] = &AccountWithBalances{
			Account: core.Account{
				Address:  account,
				Metadata: map[string]string{},
			},
			Balances: map[string]*big.Int{},
		}
	}
	c.store[account].Metadata[key] = value
}

func test(t *testing.T, tc TestCase) {
	m := NewMachine(tc.store)
	err := m.Execute(tc.program, tc.vars)
	if tc.expected.Error != "" {
		require.ErrorContains(t, err, tc.expected.Error)
		return
	}
	require.NoError(t, err)

	if tc.expected.Postings == nil {
		tc.expected.Postings = make([]Posting, 0)
	}
	if tc.expected.Metadata == nil {
		tc.expected.Metadata = make(map[string]internal.Value)
	}

	assert.Equalf(t, tc.expected.Postings, m.PostingsOuput, "unexpected postings output: %v", m.PostingsOuput)
	assert.Equalf(t, tc.expected.Metadata, m.TxMetaOutput, "unexpected metadata output: %v", m.TxMetaOutput)
	assert.Equalf(t, tc.expected.Printed, m.PrintedOutput, "unexpected printed output: %v", m.PrintedOutput)
}

func TestFail(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, "fail")
	tc.expected = CaseResult{
		Printed:  []internal.Value{},
		Postings: []Posting{},
		Error:    "failed",
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
		Error:    "insufficient",
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
	}
	test(t, tc)
}

// FIXME: Is this what we want?
func TestEmptyPostings(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM *] (
		source = @foo
		destination = @bar
	)`)
	tc.setBalance("foo", "GEM", 0)
	tc.expected = CaseResult{
		Printed:  []internal.Value{},
		Postings: []Posting{},
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
	}
	test(t, tc)
}

func TestMetadata(t *testing.T) {
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
	tc.setMeta("sales:042", "seller", "users:053")
	tc.setMeta("users:053", "commission", "125/1000")
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
		Error:    "insufficient funds",
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
	}
	test(t, tc)
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

	m := NewMachine(EmptyStore)

	err = m.Execute(*p, map[string]string{})
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

		m := NewMachine(EmptyStore)

		err = m.Execute(*p, map[string]string{})
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

		m := NewMachine(EmptyStore)

		err = m.Execute(*p, map[string]string{
			"acc": "test",
		})
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
		program, err := compiler.Compile(`
			// This is a comment
			vars {
				account $acc
			}
			set_tx_meta("account", $acc)`)
		if err != nil {
			panic(err)
		}

		m := NewMachine(&emptyStore{})

		require.NoError(t, m.Execute(*program, map[string]string{
			"acc": "valid:acc",
		}))

		require.Error(t, m.Execute(*program, map[string]string{
			"acc": "invalid-acc",
		}))

		require.NoError(t, m.Execute(*program, map[string]string{
			"acc": "valid:acc",
		}))

		require.Error(t, m.Execute(*program, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.NoError(t, m.Execute(*p, map[string]string{
			"ass": "USD/2",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"ass": "USD-2",
		}))

		require.NoError(t, m.Execute(*p, map[string]string{
			"ass": "USD/2",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.NoError(t, m.Execute(*p, map[string]string{
			"mon": "EUR/2 100",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"mon": "invalid-asset 100",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"mon": "EUR/2",
		}))

		require.NoError(t, m.Execute(*p, map[string]string{
			"mon": "EUR/2 100",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"mon": "invalid-asset 100",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.NoError(t, m.Execute(*p, map[string]string{
			"por": "1/2",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"por": "",
		}))

		require.NoError(t, m.Execute(*p, map[string]string{
			"por": "1/2",
		}))

		require.NoError(t, m.Execute(*p, map[string]string{
			"por": "50%",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"por": "3/2",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"por": "200%",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})
		require.NoError(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.NoError(t, m.Execute(*p, map[string]string{
			"nbr": "100",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
			"nbr": "string",
		}))

		require.Error(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.ErrorContains(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.ErrorContains(t, m.Execute(*p, map[string]string{
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

		m := NewMachine(&emptyStore{})

		require.ErrorContains(t, m.Execute(*p, map[string]string{
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
		Printed:  []internal.Value{},
		Postings: []Posting{},
		Error:    "negative amount",
	}
	test(t, tc)
}

// func TestMachine(t *testing.T) {
// 	p, err := compiler.Compile(`
// 		vars {
// 			account $dest
// 		}
// 		send [COIN 99] (
// 			source = @world
// 			destination = $dest
// 		)`)
// 	require.NoError(t, err)

// 	t.Run("with debug", func(t *testing.T) {
// 		m := NewMachine(*p)
// 		m.Debug = true

// 		err = m.SetVars(map[string]internal.Value{
// 			"dest": internal.AccountAddress("charlie"),
// 		})
// 		require.NoError(t, err)

// 		ch1, err := m.ResolveResources()
// 		require.NoError(t, err)
// 		for req := range ch1 {
// 			require.NoError(t, req.Error)
// 		}

// 		ch2, err := m.ResolveBalances()
// 		require.NoError(t, err)
// 		for req := range ch2 {
// 			require.NoError(t, req.Error)
// 		}

// 		exitCode, err := m.Execute()
// 		require.NoError(t, err)
// 		require.Equal(t, EXIT_OK, exitCode)
// 	})

// 	t.Run("err resources", func(t *testing.T) {
// 		m := NewMachine(*p)
// 		exitCode, err := m.Execute()
// 		require.ErrorContains(t, err, "resources haven't been initialized")
// 		require.Equal(t, byte(0), exitCode)
// 	})

// 	t.Run("err balances nit initialized", func(t *testing.T) {
// 		m := NewMachine(*p)

// 		err = m.SetVars(map[string]internal.Value{
// 			"dest": internal.AccountAddress("charlie"),
// 		})
// 		require.NoError(t, err)

// 		ch1, err := m.ResolveResources()
// 		require.NoError(t, err)
// 		for req := range ch1 {
// 			require.NoError(t, req.Error)
// 		}

// 		exitCode, err := m.Execute()
// 		require.ErrorContains(t, err, "balances haven't been initialized")
// 		require.Equal(t, byte(0), exitCode)
// 	})

// 	t.Run("err resolve resources twice", func(t *testing.T) {
// 		m := NewMachine(*p)

// 		err = m.SetVars(map[string]internal.Value{
// 			"dest": internal.AccountAddress("charlie"),
// 		})
// 		require.NoError(t, err)

// 		ch1, err := m.ResolveResources()
// 		require.NoError(t, err)
// 		for req := range ch1 {
// 			require.NoError(t, req.Error)
// 		}

// 		_, err = m.ResolveResources()
// 		require.ErrorContains(t, err, "tried to call ResolveResources twice")
// 	})

// 	t.Run("err balances before resources", func(t *testing.T) {
// 		m := NewMachine(*p)

// 		_, err := m.ResolveBalances()
// 		require.ErrorContains(t, err, "tried to resolve balances before resources")
// 	})

// 	t.Run("err resolve balances twice", func(t *testing.T) {
// 		m := NewMachine(*p)

// 		err = m.SetVars(map[string]internal.Value{
// 			"dest": internal.AccountAddress("charlie"),
// 		})
// 		require.NoError(t, err)

// 		ch1, err := m.ResolveResources()
// 		require.NoError(t, err)
// 		for req := range ch1 {
// 			require.NoError(t, req.Error)
// 		}

// 		ch2, err := m.ResolveBalances()
// 		require.NoError(t, err)
// 		for req := range ch2 {
// 			require.NoError(t, req.Error)
// 		}

// 		_, err = m.ResolveBalances()
// 		require.ErrorContains(t, err, "tried to call ResolveBalances twice")
// 	})

// 	t.Run("err missing var", func(t *testing.T) {
// 		m := NewMachine(*p)

// 		ch1, err := m.ResolveResources()
// 		require.NoError(t, err)
// 		for req := range ch1 {
// 			require.ErrorContains(t, req.Error, "missing variable 'dest'")
// 		}
// 	})
// }

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
			Error:    "negative",
		}
		test(t, tc)
	})
}
