package numscriptgen_test

import (
	"math/rand"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
)

// Every expression form the grammar offers outside feature flags shows up in
// rendered scripts.
func TestGrammarFormsAreReached(t *testing.T) {
	forms := map[string]*regexp.Regexp{
		"percentage portion":           regexp.MustCompile(`\d%`),
		"negated monetary":             regexp.MustCompile(`-\[`),
		"negative amount slot":         regexp.MustCompile(`\[\S+ -\d`),
		"var amount slot":              regexp.MustCompile(`\[\S+ \$number_\d+\]`),
		"arithmetic amount slot":       regexp.MustCompile(`\[\S+ \d+ [+-] \d+\]`),
		"monetary sum":                 regexp.MustCompile(`\] \+ \[`),
		"parenthesized monetary":       regexp.MustCompile(`\(\[`),
		"parenthesized number":         regexp.MustCompile(`\((-?\$?\w+) [+-] `),
		"empty in-order source":        regexp.MustCompile(`= \{\s*\}`),
		"in-order destination w/o max": regexp.MustCompile(`= \{\s*remaining`),
		"world with overdraft":         regexp.MustCompile(`@world allowing`),
		"monetary meta value":          regexp.MustCompile(`_meta\([^\n]*, \[`),
		"portion meta value":           regexp.MustCompile(`_meta\([^\n]*, (\d+/\d+|\d+(\.\d+)?%)\)`),
		"account meta value":           regexp.MustCompile(`_meta\([^\n]*, @`),
		"negative cap":                 regexp.MustCompile(`max (-\[|\[\S+ 0\] - |\[\S+ -)`),
	}

	counts := map[string]int{}
	for seed := range 2000 {
		_, _, _, script := gen.GenerateScript(rand.New(rand.NewSource(int64(seed))))
		for name, re := range forms {
			if re.MatchString(script) {
				counts[name]++
			}
		}
	}

	for name := range forms {
		t.Logf("%-30s %d", name, counts[name])
		require.Positive(t, counts[name], name)
	}
}
