package vm

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/machine/script/compiler"
)

func TestScriptV1IntegralScalarRepresentationsExecuteExactly(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"1000", "1000.0", "1e3", "1.00e+3", `"1000"`, "9007199254740993.0"} {
		t.Run(value, func(t *testing.T) {
			plain := "vars {\nnumber $value\n}\nset_tx_meta(\"value\", $value)\nsend [USD 1] (\nsource = @world\ndestination = @bank\n)"
			var script ScriptV1
			require.NoError(t, json.Unmarshal([]byte(`{"vars":{"value":`+value+`}}`), &script))
			converted, err := script.ToCore()
			require.NoError(t, err)
			program, err := compiler.Compile(plain)
			require.NoError(t, err)
			machine := NewMachine(*program)
			require.NoError(t, machine.SetVarsFromJSON(converted.Vars))
			require.NoError(t, machine.ResolveResources(context.Background(), StaticStore{}))
			require.NoError(t, machine.ResolveBalances(context.Background(), StaticStore{}))
			result, err := Run(machine, RunScript{Script: converted})
			require.NoError(t, err)
			want := big.NewInt(1000)
			if strings.HasPrefix(value, "9007199254740993") {
				want, _ = new(big.Int).SetString("9007199254740993", 10)
			}
			require.Equal(t, want.String(), result.Metadata["value"])
		})
	}
}

func TestScriptV1RejectsExponentExpansionBeforeConversion(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"1e1000000", "1e-1000000", "1e999999999999999999999999", "1e309"} {
		t.Run(value, func(t *testing.T) {
			var script ScriptV1
			require.NoError(t, json.Unmarshal([]byte(`{"vars":{"amount":{"asset":"USD","amount":`+value+`}}}`), &script))
			_, err := script.ToCore()
			require.ErrorIs(t, err, ErrInvalidMonetaryAmount)
		})
	}
}

func TestLegacyJSONIntegerBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ input, want string }{
		{"0e1000000", "0"}, {"-0.000e-1000000", "0"},
		{"1e308", "1" + strings.Repeat("0", 308)},
		{"1.000e3", "1000"}, {"1000000e-3", "1000"},
		{"0.000001e6", "1"}, {"-1000.00", "-1000"},
		{"0." + strings.Repeat("0", 400) + "1e401", "1"},
		{strings.Repeat("9", 309), strings.Repeat("9", 309)},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := legacyJSONInteger(json.Number(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, value := range []string{"1.01", "1e-3", "1/2", "NaN", "01", "+1", "1 ", strings.Repeat("9", 310)} {
		_, err := legacyJSONInteger(json.Number(value))
		require.Error(t, err, value)
	}
}

func TestScriptV1FractionalScalarsStillFailMachineValidation(t *testing.T) {
	t.Parallel()
	var script ScriptV1
	require.NoError(t, json.Unmarshal([]byte(`{"vars":{"value":1.25}}`), &script))
	converted, err := script.ToCore()
	require.NoError(t, err)
	program, err := compiler.Compile("vars {\nnumber $value\n}\nset_tx_meta(\"value\", $value)")
	require.NoError(t, err)
	require.Error(t, NewMachine(*program).SetVarsFromJSON(converted.Vars))
}

func BenchmarkScriptV1RejectsExponentExpansion(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var script ScriptV1
		if err := json.Unmarshal([]byte(`{"vars":{"a":{"asset":"USD","amount":1e1000000},"b":{"asset":"USD","amount":1e1000000},"c":{"asset":"USD","amount":1e1000000},"d":{"asset":"USD","amount":1e1000000}}}`), &script); err != nil {
			b.Fatal(err)
		}
		if _, err := script.ToCore(); err == nil {
			b.Fatal("expanded amount was accepted")
		}
	}
}
