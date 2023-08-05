package vm

import (
	"testing"

	"github.com/numary/ledger/pkg/core"
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1),
				Source:      "a",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(24),
				Source:      "world",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(25),
				Source:      "world",
				Destination: "y",
			},
		},
	}
	test(t, tc)
}

func TestKeptDestinationInOrder(t *testing.T) {
	tc := NewTestCase()
	tc.compile(t, `send [GEM 100] (
		source = {
			@a
			@world
		}
		destination = {
			max [GEM 8] to @x
			remaining kept
		}
	)`)
	tc.setBalance("a", "GEM", 1)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(1),
				Source:      "a",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(7),
				Source:      "world",
				Destination: "x",
			},
		},
	}
	test(t, tc)
}

func TestKeptComplex(t *testing.T) {
	tc := NewTestCase()
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
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(2),
				Source:      "foo",
				Destination: "arst",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(18),
				Source:      "foo",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(24),
				Source:      "bar",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(16),
				Source:      "bar",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(4),
				Source:      "baz",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      core.NewMonetaryInt(25),
				Source:      "baz",
				Destination: "quz",
			},
		},
	}
	test(t, tc)
}
