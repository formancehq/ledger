package vm

import (
	"testing"

	"github.com/formancehq/ledger/internal/machine"
)

func TestKeptDestinationAllotment(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
		source = {
			@a
			@world
		}
		destination = {
			50% kept
			25% to @x
			25% to @y
		}
	)`)
	tc.setBalance("a", "GEM", 1)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(1),
				Source:      "a",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(24),
				Source:      "world",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(25),
				Source:      "world",
				Destination: "y",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestKeptComplex(t *testing.T) {
	tc := NewTestCase()
	// [GEM | @foo 20, @bar 40, @baz 40]
	tc.compile(t, `send [GEM 100] (
			source = {
				@foo
				@bar
				@baz
			}
			destination = {
				50% to {
					max [GEM 8] to {
						50% kept
						25% to @arst
						25% kept
					}
					remaining to @thing
				}
				20% to @qux
				5% kept
				remaining to @quz
			}
		)`)
	tc.setBalance("foo", "GEM", 20)
	tc.setBalance("bar", "GEM", 40)
	tc.setBalance("baz", "GEM", 40)
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(2),
				Source:      "foo",
				Destination: "arst",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(18),
				Source:      "foo",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(24),
				Source:      "bar",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(16),
				Source:      "bar",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(4),
				Source:      "baz",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      machine.NewMonetaryInt(25),
				Source:      "baz",
				Destination: "quz",
			},
		},
		Error: nil,
	}
	test(t, tc)
}

func TestTwoConsecutiveKepts(t *testing.T) {
	tc := NewTestCase()

	tc.compile(t, `
		send [COIN 100] (
			source = @acc0
			destination = {
				max [COIN 60] to @acc1
				max [COIN 40] kept
				max [COIN 50] kept
				remaining to @acc2
			}
		)
	`)

	tc.balances = map[string]map[string]*machine.MonetaryInt{
		"acc0": {
			"COIN": machine.NewMonetaryInt(100),
		},
	}
	tc.expected = CaseResult{
		Printed: []machine.Value{},
		Postings: []Posting{
			{"acc0", "acc1", machine.NewMonetaryInt(60), "COIN"},
		},
	}
	test(t, tc)
}
