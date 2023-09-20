package vm

import (
	"testing"

	internal2 "github.com/formancehq/ledger/internal/machine/internal"
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
		Printed: []internal2.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(1),
				Source:      "a",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(24),
				Source:      "world",
				Destination: "x",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(25),
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
		Printed: []internal2.Value{},
		Postings: []Posting{
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(2),
				Source:      "foo",
				Destination: "arst",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(18),
				Source:      "foo",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(24),
				Source:      "bar",
				Destination: "thing",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(16),
				Source:      "bar",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(4),
				Source:      "baz",
				Destination: "qux",
			},
			{
				Asset:       "GEM",
				Amount:      internal2.NewMonetaryInt(25),
				Source:      "baz",
				Destination: "quz",
			},
		},
		Error: nil,
	}
	test(t, tc)
}
