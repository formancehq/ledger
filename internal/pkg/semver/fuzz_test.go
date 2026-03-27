package semver

import (
	"testing"
)

// FuzzSemverParse fuzzes the strict semver parser (major.minor.patch).
func FuzzSemverParse(f *testing.F) {
	f.Add("0.0.0")
	f.Add("1.0.0")
	f.Add("1.2.3")
	f.Add("255.255.255")
	f.Add("4294967295.4294967295.4294967295") // max uint32
	f.Add("4294967296.0.0")                   // overflow uint32
	f.Add("0.0")
	f.Add("1")
	f.Add("")
	f.Add("a.b.c")
	f.Add("1.2.3.4")
	f.Add("-1.0.0")
	f.Add("1..0")
	f.Add(".1.0")
	f.Add("1.0.")
	f.Add("v1.0.0")
	f.Add("1.0.0-beta")
	f.Add("1.0.0+build")

	f.Fuzz(func(t *testing.T, input string) {
		v, err := Parse(input)
		if err != nil {
			return
		}

		// If parsing succeeded, verify round-trip.
		s := v.String()

		v2, err := Parse(s)
		if err != nil {
			t.Fatalf("round-trip Parse failed: %q -> %q: %v", input, s, err)
		}

		if v != v2 {
			t.Fatalf("round-trip mismatch: %q -> %v -> %q -> %v", input, v, s, v2)
		}
	})
}

// FuzzSemverParsePartial fuzzes the partial semver parser (1 to 3 components).
func FuzzSemverParsePartial(f *testing.F) {
	f.Add("1")
	f.Add("1.2")
	f.Add("1.2.3")
	f.Add("0")
	f.Add("0.0")
	f.Add("0.0.0")
	f.Add("")
	f.Add("abc")
	f.Add("1.2.3.4")
	f.Add("-1")
	f.Add("1.")
	f.Add(".1")

	f.Fuzz(func(t *testing.T, input string) {
		major, minor, patch, depth, err := ParsePartial(input)
		if err != nil {
			return
		}

		// Verify depth makes sense.
		if depth < 1 || depth > 3 {
			t.Fatalf("invalid depth %d for input %q", depth, input)
		}

		// Verify re-parsing the full version gives the same values.
		v, err := Parse(Version{Major: major, Minor: minor, Patch: patch}.String())
		if err != nil {
			t.Fatalf("re-parse failed: %v", err)
		}

		if v.Major != major || v.Minor != minor || v.Patch != patch {
			t.Fatalf("mismatch: input=%q depth=%d -> %d.%d.%d but re-parse gives %v",
				input, depth, major, minor, patch, v)
		}
	})
}
