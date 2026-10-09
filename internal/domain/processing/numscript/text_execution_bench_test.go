package numscript

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkFSMTextSmall(b *testing.B)  { benchmarkFSMText(b, benchSmall, nil) }
func BenchmarkFSMTextMedium(b *testing.B) { benchmarkFSMText(b, benchMedium, benchMediumVars) }
func BenchmarkFSMTextLarge(b *testing.B) {
	benchmarkFSMText(b, strings.Repeat(benchSmall+"\n", 12), nil)
}

// The same script with changing business variables is the recommended usage
// pattern. The script-dependent compile and VM remain warm across orders.
func BenchmarkFSMTextChangingVars(b *testing.B) {
	cache := NewNumscriptCache(16)
	store := NewVMStore(benchSource(), false)
	vars := make([]map[string]string, 64)
	for i := range vars {
		vars[i] = map[string]string{"amt": "USD/2 900", "dest": fmt.Sprintf("out_%d", i)}
	}
	if _, err := SafeExecFromText(cache, benchMedium, vars[0], store); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := SafeExecFromText(cache, benchMedium, vars[i%len(vars)], store); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// More distinct scripts than cache slots keeps both LRUs under eviction.
func BenchmarkFSMTextScriptChurn(b *testing.B) {
	cache := NewNumscriptCache(16)
	store := NewVMStore(benchSource(), false)
	scripts := make([]string, 64)
	for i := range scripts {
		scripts[i] = fmt.Sprintf("send [USD/2 100] (source = @wallet destination = @out_%d)", i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := SafeExecFromText(cache, scripts[i%len(scripts)], nil, store); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func benchmarkFSMText(b *testing.B, script string, vars map[string]string) {
	source := benchSource()
	b.Run("warm", func(b *testing.B) {
		cache := NewNumscriptCache(16)
		store := NewVMStore(source, false)
		if _, err := SafeExecFromText(cache, script, vars, store); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SafeExecFromText(cache, script, vars, store); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cold", func(b *testing.B) {
		store := NewVMStore(source, false)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := SafeExecFromText(NewNumscriptCache(16), script, vars, store); err != nil {
				b.Fatal(err)
			}
		}
	})
}
