package numscriptgen_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
)

// Every value type must be reachable in both var and inline form, and the two
// forms must co-occur inside one script — that co-occurrence is what makes two
// resources alias one account (formancehq/ledger#2056).
//
// A portion var only compiles inside an allotment ending in `remaining` (see
// toBuilderPortion), so its var-form count is bounded by how often that block
// shape is drawn.
func TestEveryValueTypeReachesBothForms(t *testing.T) {
	varRe := map[string]*regexp.Regexp{
		"account":  regexp.MustCompile(`\$account_\d+`),
		"asset":    regexp.MustCompile(`\$asset_\d+`),
		"number":   regexp.MustCompile(`\$number_\d+`),
		"string":   regexp.MustCompile(`\$string_\d+`),
		"monetary": regexp.MustCompile(`\$monetary_\d+`),
		"portion":  regexp.MustCompile(`\$portion_\d+`),
	}
	litRe := map[string]*regexp.Regexp{
		"account":  regexp.MustCompile(`@(acc\d+|world)`),
		"asset":    regexp.MustCompile(`\b(COIN|USD/2|EUR/2)\b`),
		"number":   regexp.MustCompile(`"k\d+", \d`),
		"string":   regexp.MustCompile(`"str\d+"`),
		"monetary": regexp.MustCompile(`\[[^\]]+ \d+\]`),
		"portion":  regexp.MustCompile(`\d+/\d+ (to|from|kept)`),
	}

	const n = 800
	varSeen := map[string]int{}
	litSeen := map[string]int{}
	bothSeen := map[string]int{}
	worldBoth, alias := 0, 0

	for s := range n {
		vars, _, _, script := gen.GenerateScript(gen.RandFromBytes([]byte(fmt.Sprintf("s%d", s))))
		for k, re := range varRe {
			v := re.MatchString(script)
			l := litRe[k].MatchString(script)
			if v {
				varSeen[k]++
			}
			if l {
				litSeen[k]++
			}
			if v && l {
				bothSeen[k]++
			}
		}
		wv := false
		for _, v := range vars {
			if v == "world" {
				wv = true
			}
		}
		if wv && strings.Contains(script, "@world") {
			worldBoth++
		}
		if strings.Contains(script, "balance($account") && regexp.MustCompile(`@acc\d+`).MatchString(script) {
			alias++
		}
	}

	for _, k := range []string{"account", "asset", "number", "string", "monetary", "portion"} {
		t.Logf("%-9s var=%3d  inline=%3d  both-in-one-script=%3d  (of %d)", k, varSeen[k], litSeen[k], bothSeen[k], n)
		if litSeen[k] == 0 {
			t.Errorf("%s never generated in inline form", k)
		}
		if varSeen[k] == 0 {
			t.Errorf("%s never generated in var form", k)
		}
	}
	t.Logf("@world as literal AND as a var value  : %d/%d", worldBoth, n)
	t.Logf("balance($account_N) + @accN literals  : %d/%d", alias, n)
	if worldBoth == 0 {
		t.Error("@world never appears both inline and as a var value")
	}
}
