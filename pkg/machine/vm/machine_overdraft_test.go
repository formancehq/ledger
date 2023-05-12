package vm

import (
	"testing"

	"github.com/formancehq/ledger/pkg/core"
)

func TestOverdraftNotEnough(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
		source = @foo allowing overdraft up to [GEM 10]
		destination = @world
	)`)
	tc.setBalance("foo", "GEM", 89)
	tc.expected = CaseResult{
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
	}
	test(t, tc)
}

func TestOverdraftEnough(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
			source = @foo allowing overdraft up to [GEM 10]
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", 90)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(100),
				Source:      "foo",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestOverdraftUnbounded(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 1000] (
			source = @foo allowing unbounded overdraft
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", 90)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1000),
				Source:      "foo",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestOverdraftSourceAllotmentSuccess(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
			source = {
				50% from @foo allowing overdraft up to [GEM 10]
				50% from {
					@bar allowing overdraft up to [GEM 20]
					@baz allowing unbounded overdraft
				}
			}
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", 40)
	tc.setBalance("bar", "GEM", 20)
	tc.setBalance("baz", "GEM", 0)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(50),
				Source:      "foo",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(40),
				Source:      "bar",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(10),
				Source:      "baz",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestOverdraftSourceInOrderSuccess(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
			source = {
				max [GEM 50] from {
					@foo allowing overdraft up to [GEM 10]
					@bar allowing overdraft up to [GEM 20]
					@baz allowing unbounded overdraft
				}
				@qux allowing unbounded overdraft
			}
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", 0)
	tc.setBalance("bar", "GEM", 0)
	tc.setBalance("baz", "GEM", 0)
	tc.setBalance("qux", "GEM", 0)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(10),
				Source:      "foo",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(20),
				Source:      "bar",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(20),
				Source:      "baz",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(50),
				Source:      "qux",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestOverdraftBalanceTracking(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
		source = @foo allowing unbounded overdraft
		destination = @world
	)
	send [GEM 200] (
		source = @foo allowing overdraft up to [GEM 300]
		destination = @world
	)
	send [GEM 300] (
		source = @foo allowing unbounded overdraft
		destination = @world
	)
	`)
	tc.setBalance("foo", "GEM", 0)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(100),
				Source:      "foo",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(200),
				Source:      "foo",
				Destination: "world",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(300),
				Source:      "foo",
				Destination: "world",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestWorldIsUnbounded(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
		source = @world
		destination = @foo
	)
	send [GEM 200] (
		source = @world
		destination = @foo
	)
	`)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(100),
				Source:      "world",
				Destination: "foo",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(200),
				Source:      "world",
				Destination: "foo",
			},
		},
		ExitCode: EXIT_OK,
	}
	test(t, tc)
}

func TestOverdraftComplexFailure(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
			source = {
				50% from @foo allowing overdraft up to [GEM 10]
				50% from {
					@bar allowing overdraft up to [GEM 20]
					@baz
				}
			}
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", 40)
	tc.setBalance("bar", "GEM", 20)
	tc.setBalance("baz", "GEM", 0)
	tc.expected = CaseResult{
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
	}
	test(t, tc)
}

func TestNegativeBalance(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
			source = @foo
			destination = @world
		)`)
	tc.setBalance("foo", "GEM", -50)
	tc.expected = CaseResult{
		Printed:  []core.Value{},
		Postings: []Posting{},
		ExitCode: EXIT_FAIL_INSUFFICIENT_FUNDS,
	}
	test(t, tc)
}
