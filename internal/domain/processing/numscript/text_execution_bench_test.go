package numscript

import "testing"

func BenchmarkFSMTextSmall(b *testing.B)  { benchmarkFSMText(b, benchSmall, nil) }
func BenchmarkFSMTextMedium(b *testing.B) { benchmarkFSMText(b, benchMedium, benchMediumVars) }

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
