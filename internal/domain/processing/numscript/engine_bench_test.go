package numscript

// Benchmarks the two FSM execution engines against each other, plus the VM
// path's decode/verify/exec decomposition. The decomposition is what justifies
// GetOrDecodeCompiled: the verifier is a whole-program static pass costing
// several times a full interpreter run (and ~30x an execution), so paying it
// per apply would make the VM path slower than interpreting — cached per
// artifact, the VM path is roughly twice as fast as the cached-parse
// interpreter on these scripts.

import (
	"context"
	"maps"
	"math/big"
	"testing"

	numscriptlib "github.com/formancehq/numscript"
)

const benchSmall = `send [USD/2 100] (
  source = @wallet
  destination = @out
)`

const benchMedium = `vars {
  monetary $amt
  account $dest
}

send $amt (
  source = {
    max [USD/2 500] from @wallet:main
    @wallet:reserve
  }
  destination = {
    2/3 to $dest
    remaining to @fees
  }
)

send [USD/2 40] (
  source = @wallet:main
  destination = {
    max [USD/2 10] to @fees
    remaining to @out
  }
)

set_tx_meta("kind", "payment")
set_account_meta(@out, "last", "bench")`

var benchMediumVars = map[string]string{"amt": "USD/2 900", "dest": "out"}

func benchSource() mapValueSource {
	return mapValueSource{balances: map[string]*big.Int{
		"wallet\x00USD/2\x00":         big.NewInt(100000),
		"wallet:main\x00USD/2\x00":    big.NewInt(100000),
		"wallet:reserve\x00USD/2\x00": big.NewInt(100000),
	}}
}

func benchCase(b *testing.B, script string, vars map[string]string) {
	parsed := numscriptlib.Parse(script)
	if len(parsed.GetParsingErrors()) > 0 {
		b.Fatal("parse errors")
	}

	compiled := compileScript(parsed, script, vars)
	if compiled == nil {
		b.Fatal("compile failed")
	}

	source := benchSource()
	cache := NewNumscriptCache(16)

	b.Run("interpreter_cached_parse", func(b *testing.B) {
		store := NewStore(source, false)
		vm := make(numscriptlib.VariablesMap, len(vars))
		maps.Copy(vm, vars)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SafeRun(parsed, context.Background(), vm, store); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vm_decode_verify_exec", func(b *testing.B) {
		store := NewVMStore(source, false)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SafeExecCompiled(cache, compiled.Program, compiled.Vars, store); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vm_decode_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := numscriptlib.DecodeCompiledProgram(compiled.Program); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vm_verify_only", func(b *testing.B) {
		program, _ := numscriptlib.DecodeCompiledProgram(compiled.Program)
		encodedVars, _ := numscriptlib.DecodeVars(compiled.Vars)
		b.ReportAllocs()
		for b.Loop() {
			if err := numscriptlib.VerifyCompiledProgramWithVars(program, &encodedVars); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vm_exec_only", func(b *testing.B) {
		program, _ := numscriptlib.DecodeCompiledProgram(compiled.Program)
		encodedVars, _ := numscriptlib.DecodeVars(compiled.Vars)
		store := NewVMStore(source, false)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := numscriptlib.ExecVm(context.Background(), numscriptlib.NewVm(program), &encodedVars, store); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFSMEngineSmall(b *testing.B)  { benchCase(b, benchSmall, nil) }
func BenchmarkFSMEngineMedium(b *testing.B) { benchCase(b, benchMedium, benchMediumVars) }
