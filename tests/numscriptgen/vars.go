package numscriptgen

import (
	"fmt"
	"math/rand"
	"strings"
)

// RerollVars redraws some of a program's runtime-bound var values, so a stored
// script runs again on other inputs. The builder names every runtime var after
// its type (account_N, asset_N, ...), which is what picks the new value's shape;
// a monetary keeps its asset, so the statements it feeds stay asset-consistent.
// Account values are drawn from accounts, or world.
func RerollVars(rng *rand.Rand, vars map[string]string, accounts []string) map[string]string {
	out := make(map[string]string, len(vars))
	for name, value := range vars {
		out[name] = value

		if rng.Intn(2) == 0 {
			continue
		}

		kind, _, _ := strings.Cut(name, "_")
		switch kind {
		case "account":
			if len(accounts) == 0 || rng.Intn(5) == 0 {
				out[name] = "world"
			} else {
				out[name] = accounts[rng.Intn(len(accounts))]
			}
		case "asset":
			out[name] = pickAsset(rng)
		case "number":
			out[name] = fmt.Sprintf("%d", rng.Intn(2000)-500)
		case "monetary":
			asset, _, _ := strings.Cut(value, " ")
			out[name] = fmt.Sprintf("%s %d", asset, rng.Intn(1000))
		case "portion":
			den := rng.Intn(9) + 2
			out[name] = fmt.Sprintf("%d/%d", rng.Intn(den+1), den)
		case "string":
			out[name] = fmt.Sprintf("str%d", rng.Intn(4))
		}
	}

	return out
}
