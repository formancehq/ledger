package numscriptgen

import (
	"hash/fnv"
	"math/big"
	"math/rand"

	"github.com/formancehq/numscript/builder"
)

// Feature flags a generated script may declare in its #![feature(...)] header.
const (
	FlagOneof       = "experimental-oneof"
	FlagAssetColors = "experimental-asset-colors"
)

// RandFromBytes derives an RNG from arbitrary bytes, so a Go fuzz target can
// drive generation reproducibly from its []byte corpus.
func RandFromBytes(b []byte) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write(b) // hash.Hash.Write never errors
	return rand.New(rand.NewSource(int64(h.Sum64())))
}

// Generated is one runnable numscript program with the vars bindings it needs
// at run time, the starting balances and metadata it assumes, and the
// structural Shape of the program.
type Generated struct {
	Vars     map[string]string
	Balances map[BalanceKey]*big.Int
	Metadata map[MetaKey]string
	Script   string
	Shape    Shape

	// Flags are the feature flags the script needs on the numscript engines
	// (interpreter and compiler+VM). Derived from Shape.
	Flags []string
	// OracleCompatible is false when the script uses shapes outside the legacy
	// machine's grammar or decided behavior (oneof, colors, division portions,
	// wrong-asset caps): the harness then runs only the two numscript engines
	// and compares them against each other.
	OracleCompatible bool
}

// Generate formats a random Script as a runnable numscript program. The
// caller loads Balances and Metadata into each engine store before executing.
func Generate(rng *rand.Rand) Generated {
	return render(generateScriptAST(rng))
}

// GenerateFor is Generate over the caller's accounts: acc<i> becomes
// accounts[i], so accounts needs at least maxPoolSize entries. Entries may
// repeat, which aliases two pool accounts onto one address.
func GenerateFor(rng *rand.Rand, accounts []string) Generated {
	s := generateScriptAST(rng)
	newAccountRenamer(accounts).script(&s)

	return render(s)
}

// render builds the program text, declaring the feature flags its shape needs
// in the script header.
func render(s Script) Generated {
	shape := computeShape(s)
	var featureFlags []string
	if shape.HasOneof {
		featureFlags = append(featureFlags, FlagOneof)
	}
	if shape.HasColoredSource {
		featureFlags = append(featureFlags, FlagAssetColors)
	}

	stmts, accountVarFills := toBuilderScript(s)

	build := builder.BuildProgram
	if len(featureFlags) > 0 {
		build = func(stmts ...builder.Statement) (map[string]string, builder.VarsEnv, string) {
			return builder.BuildProgramWithFeatureFlags(featureFlags, stmts...)
		}
	}

	vars, varsEnv, script := build(stmts...)

	// Account-typed vars have no compiler-computed origin, so their value is
	// supplied here the way a real caller would.
	for _, f := range accountVarFills {
		name, value := varsEnv.FillAccount(f.Var, f.Value)
		if name != "" { // "" means this var was declared but never referenced
			vars[name] = value
		}
	}

	return Generated{
		Vars:     vars,
		Balances: s.Balances,
		Metadata: s.Metadata,
		Script:   script,
		Shape:    shape,

		Flags:            featureFlags,
		OracleCompatible: !shape.HasOneof && !shape.HasColoredSource && !shape.HasWrongAssetCap && !shape.HasPortionDiv,
	}
}

// GenerateScript is Generate without the Shape.
func GenerateScript(rng *rand.Rand) (vars map[string]string, balances map[BalanceKey]*big.Int, metadata map[MetaKey]string, script string) {
	g := Generate(rng)
	return g.Vars, g.Balances, g.Metadata, g.Script
}
