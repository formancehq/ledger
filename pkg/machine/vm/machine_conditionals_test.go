package vm

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/numary/ledger/pkg/core"
)

func TestConditionals(t *testing.T) {
	script := `
	vars {
		bool $foo
	}
	send [COIN 1] (
		source = @world
		destination = $foo ? @istrue : @isfalse
	)
	`
	tc := NewTestCase()
	tc.compile(t, script)
	// check true
	tc.setVarsFromJSON(t, `{
		"foo": true
	}`)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Source:      "world",
				Destination: "istrue",
				Amount:      core.NewMonetaryInt(1),
				Asset:       "COIN",
			},
		},
	}
	// check false
	tc.setVarsFromJSON(t, `{
		"foo": false
	}`)
	tc.expected = CaseResult{
		Printed: []core.Value{},
		Postings: []Posting{
			{
				Source:      "world",
				Destination: "isfalse",
				Amount:      core.NewMonetaryInt(1),
				Asset:       "COIN",
			},
		},
	}
	test(t, tc)
}

func TestInequality(t *testing.T) {
	script := `
	vars {
		number $foo
		number $bar
	}
	print $foo > $bar && !($foo == $bar) ? @istrue : @isfalse
	`
	tc := NewTestCase()
	tc.compile(t, script)
	b, _ := json.MarshalIndent(tc.program, "", "\t")
	fmt.Printf("%v\n", string(b))
	tc.setVarsFromJSON(t, `{
		"foo": 42,
		"bar": 30
	}`)
	tc.expected = CaseResult{
		Printed: []core.Value{
			core.AccountAddress("istrue"),
		},
		Postings: []Posting{},
	}
	test(t, tc)
}
