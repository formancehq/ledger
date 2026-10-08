package numscript

// Benchmarks the FSM's VM execution path and its decode/verify/exec
// decomposition.

import (
	"context"
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
	cache := NewNumscriptCache(16)

	entry := cache.getOrParseEntry(script)
	if entry.script.err != nil {
		b.Fatal("parse errors")
	}

	compiled, compileErr := compileScript(entry, vars)
	if compileErr != nil {
		b.Fatal(compileErr)
	}

	source := benchSource()

	// warm_vm vs fresh_vm is the A/B for reusing the cached VM instance: both
	// decode the vars and hit the decode+verify cache exactly as apply does,
	// and differ only in executing on the entry's warm instance or on a new
	// one built from the same verified program.
	b.Run("warm_vm", func(b *testing.B) {
		store := NewVMStore(source, false)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SafeExecCompiled(cache, compiled.ScriptHash, compiled.Program, compiled.Vars, store); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("fresh_vm", func(b *testing.B) {
		store := NewVMStore(source, false)
		b.ReportAllocs()
		for b.Loop() {
			vars, decErr := numscriptlib.DecodeVars(compiled.Vars)
			if decErr != nil {
				b.Fatal(decErr)
			}

			entry, err := cache.getOrDecodeCompiled(compiled.ScriptHash, compiled.Program, &vars)
			if err != nil {
				b.Fatal(err)
			}

			if _, err := safeExecVM(numscriptlib.NewVm(entry.vm.Program), &vars, store); err != nil {
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
			if _, err := numscriptlib.VerifyCompiledProgramWithVars(program, &encodedVars); err != nil {
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
