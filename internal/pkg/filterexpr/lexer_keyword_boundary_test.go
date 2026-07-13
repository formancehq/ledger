package filterexpr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// demotedKeywords are the noun/field words that EN-1547 demoted from lexer
// keywords to Ident-matched grammar literals, so an identifier that merely starts
// with one no longer mis-tokenizes.
var demotedKeywords = []string{
	"metadata", "address", "source", "destination", "ledger", "audit", "exists",
	"true", "false",
}

// identContinuations are the Ident-continuation characters that Go's `\b` treats
// as word boundaries — the exact set that made a keyword `\b` match inside an
// identifier before EN-1547.
var identContinuations = []string{"-", ":", ".", "/"}

// TestLexerKeywordBoundary_MetadataKey is the regression matrix for EN-1547: a
// metadata key that starts with a (now-demoted) noun word and continues with a
// punctuation continuation char must parse. Before the fix the keyword `\b`
// boundary matched before the punctuation, splitting the identifier and rejecting
// the filter.
func TestLexerKeywordBoundary_MetadataKey(t *testing.T) {
	t.Parallel()

	for _, kw := range demotedKeywords {
		for _, sep := range identContinuations {
			key := kw + sep + "suffix"
			in := fmt.Sprintf(`metadata[%s] == "v"`, key)

			t.Run(in, func(t *testing.T) {
				t.Parallel()

				f, err := Parse(in)
				require.NoError(t, err, in)

				fc := f.GetField()
				require.NotNil(t, fc, in)
				require.Equal(t, key, fc.GetField().GetMetadata(), in)
			})
		}
	}
}

// TestLexerKeywordBoundary_BareValue is the symmetric matrix for the value
// position: a bare (unquoted) value that starts with a demoted noun word and
// continues with a punctuation char must parse as that full identifier string.
func TestLexerKeywordBoundary_BareValue(t *testing.T) {
	t.Parallel()

	for _, kw := range demotedKeywords {
		for _, sep := range identContinuations {
			val := kw + sep + "1"
			in := "metadata[k] == " + val

			t.Run(in, func(t *testing.T) {
				t.Parallel()

				f, err := Parse(in)
				require.NoError(t, err, in)

				sc := f.GetField().GetStringCond()
				require.NotNil(t, sc, in)
				require.Equal(t, val, sc.GetHardcoded(), in)
			})
		}
	}
}

// TestLexerKeywordBoundary_DemotedWordsAsBareValues confirms each demoted noun
// word is usable as a plain unquoted value (it lexes as Ident now).
func TestLexerKeywordBoundary_DemotedWordsAsBareValues(t *testing.T) {
	t.Parallel()

	for _, kw := range []string{"metadata", "address", "source", "destination", "ledger", "audit", "exists"} {
		in := "metadata[type] == " + kw

		t.Run(in, func(t *testing.T) {
			t.Parallel()

			f, err := Parse(in)
			require.NoError(t, err, in)
			require.Equal(t, kw, f.GetField().GetStringCond().GetHardcoded(), in)
		})
	}
}

// TestLexerKeywordBoundary_ExistingFormsStillParse locks that every pre-existing
// DSL form still parses after the demotion — the noun words keep working as field
// selectors, and the structural operators still combine/terminate expressions.
func TestLexerKeywordBoundary_ExistingFormsStillParse(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`metadata[k] == v`,
		`metadata[status] == "active"`,
		`address == "users:alice"`,
		`address ^= "users:"`,
		`source == "acc"`,
		`destination in ("a", "b")`,
		`audit[timestamp] > 100`,
		`audit[outcome] == failure`,
		`ledger == "main"`,
		`metadata[cat] exists`,
		`metadata[a] == "x" and metadata[b] == "y"`,
		`metadata[a] == "x" or metadata[b] == "y"`,
		`not metadata[a] == "x"`,
		`metadata[n] in (1, 2, 3)`,
		`metadata[age] between 18 and 65`,
		`has asset USD`,
		`has asset USD/2`,
		`metadata[flag] == true`,
		`metadata[flag] == false`,
		// structural-operator words remain usable as metadata KEYS (Keyword alt)
		`metadata[and] == "x"`,
		`metadata[between] == "x"`,
		`metadata[in] == "x"`,
	} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(in)
			require.NoError(t, err, in)
		})
	}
}

// TestLexerKeywordBoundary_StructuralOperatorsStillReserved confirms the
// structural operators are NOT usable as bare values — they must keep terminating
// expressions. `metadata[k] == and` is a parse error, not a value of "and".
func TestLexerKeywordBoundary_StructuralOperatorsStillReserved(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"and", "or", "not", "in", "between"} {
		in := "metadata[k] == " + op

		t.Run(in, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(in)
			require.Error(t, err, in)
		})
	}
}

// TestLexerKeywordBoundary_ReservedValueRoundTrips locks the Format side: a value
// equal to a reserved structural operator must be quoted by Format so it
// round-trips, while a demoted noun word is emitted bare and still round-trips.
func TestLexerKeywordBoundary_ReservedValueRoundTrips(t *testing.T) {
	t.Parallel()

	for _, val := range []string{
		"in", "and", "or", "not", "between", // reserved -> quoted
		"audit", "ledger", "source", "metadata", // demoted -> bare
		"date-range", "source.id", // punctuated identifiers
		"123abc", "10", // leading digit -> must quote (not a bare Ident)
		"a,b", "x=y", "a@b", "a>b", "a b", // special chars -> must quote
	} {
		t.Run(val, func(t *testing.T) {
			t.Parallel()

			f, err := Parse(fmt.Sprintf(`metadata[k] == "%s"`, val))
			require.NoError(t, err)

			out := Format(f)
			reparsed, err := Parse(out)
			require.NoError(t, err, "Format output %q must reparse", out)
			require.True(t, proto.Equal(f, reparsed),
				"round-trip mismatch for %q: Format=%q", val, out)
		})
	}
}
