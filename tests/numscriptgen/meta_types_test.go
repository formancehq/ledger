package numscriptgen_test

import (
	"fmt"
	"regexp"
	"testing"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
)

// meta() is typed by its declaration, not by the stored value, so every one of
// the six declarable types has to be generated against a value that parses as
// it. Before this was threaded through, every meta() read was a number and the
// other five paths were dead.
func TestMetaReadsCoverEveryType(t *testing.T) {
	re := regexp.MustCompile(`(\w+) \$originvar_\d+ = meta\(`)
	seen := map[string]int{}
	const n = 4000
	for s := range n {
		_, _, _, script := gen.GenerateScript(gen.RandFromBytes([]byte(fmt.Sprintf("m%d", s))))
		for _, m := range re.FindAllStringSubmatch(script, -1) {
			seen[m[1]]++
		}
	}
	for _, typ := range []string{"number", "string", "monetary", "asset", "account", "portion"} {
		t.Logf("%-9s meta() reads: %d", typ, seen[typ])
		if seen[typ] == 0 {
			t.Errorf("meta() is never read as %s", typ)
		}
	}
}
