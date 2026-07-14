package filterexpr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// specialChars are the characters that used to be legal in a bare Ident and are
// now excluded (EN-1547), so any key/value containing one must be quoted.
var specialChars = []string{"-", ":", ".", "/"}

// TestQuoting_MetadataKeyRequiresQuotingForSpecialChars asserts a metadata key
// containing a special char parses ONLY when quoted, and is rejected bare.
func TestQuoting_MetadataKeyRequiresQuotingForSpecialChars(t *testing.T) {
	t.Parallel()

	for _, sep := range specialChars {
		key := "x" + sep + "id"

		t.Run(key, func(t *testing.T) {
			t.Parallel()

			// Quoted: parses, key preserved verbatim.
			quoted := fmt.Sprintf(`metadata["%s"] == "v"`, key)
			f, err := Parse(quoted)
			require.NoError(t, err, quoted)
			require.Equal(t, key, f.GetField().GetField().GetMetadata(), quoted)

			// Bare: rejected.
			bare := fmt.Sprintf(`metadata[%s] == "v"`, key)
			_, err = Parse(bare)
			require.Error(t, err, bare)
		})
	}
}

// TestQuoting_ValueRequiresQuotingForSpecialChars asserts a value containing a
// special char parses ONLY when quoted (metadata value, address, ledger), and is
// rejected bare.
func TestQuoting_ValueRequiresQuotingForSpecialChars(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		quotedTmpl string
		bareTmpl   string
		get        func(*commonpb.QueryFilter) string
	}{
		{
			name:       "metadata value",
			quotedTmpl: `metadata[k] == "%s"`,
			bareTmpl:   `metadata[k] == %s`,
			get:        func(f *commonpb.QueryFilter) string { return f.GetField().GetStringCond().GetHardcoded() },
		},
		{
			name:       "address exact",
			quotedTmpl: `address == "%s"`,
			bareTmpl:   `address == %s`,
			get:        func(f *commonpb.QueryFilter) string { return f.GetAddress().GetHardcodedExact() },
		},
		{
			name:       "address prefix",
			quotedTmpl: `address ^= "%s"`,
			bareTmpl:   `address ^= %s`,
			get:        func(f *commonpb.QueryFilter) string { return f.GetAddress().GetHardcodedPrefix() },
		},
		{
			name:       "ledger value",
			quotedTmpl: `ledger == "%s"`,
			bareTmpl:   `ledger == %s`,
			get:        func(f *commonpb.QueryFilter) string { return f.GetLedger().GetCond().GetHardcoded() },
		},
	}

	for _, tc := range cases {
		for _, sep := range specialChars {
			val := "a" + sep + "b"

			t.Run(tc.name+"_"+sep, func(t *testing.T) {
				t.Parallel()

				quoted := fmt.Sprintf(tc.quotedTmpl, val)
				f, err := Parse(quoted)
				require.NoError(t, err, quoted)
				require.Equal(t, val, tc.get(f), quoted)

				bare := fmt.Sprintf(tc.bareTmpl, val)
				_, err = Parse(bare)
				require.Error(t, err, bare)
			})
		}
	}
}

// TestQuoting_AuditValueRequiresQuoting asserts a punctuated audit field value
// (e.g. caller_subject "svc:payments", ledger "my-ledger") parses only quoted.
func TestQuoting_AuditValueRequiresQuoting(t *testing.T) {
	t.Parallel()

	f, err := Parse(`audit[caller_subject] == "svc:payments"`)
	require.NoError(t, err)
	require.Equal(t, "svc:payments", f.GetAudit().GetStringCond().GetHardcoded())

	_, err = Parse("audit[caller_subject] == svc:payments")
	require.Error(t, err)

	f, err = Parse(`audit[ledger] == "my-ledger"`)
	require.NoError(t, err)
	require.Equal(t, "my-ledger", f.GetAudit().GetStringCond().GetHardcoded())

	_, err = Parse("audit[ledger] == my-ledger")
	require.Error(t, err)
}

// TestQuoting_AssetRefStillBare confirms the one structured exception: the
// base/precision asset reference USD/2 remains expressible bare (its own AssetRef
// token), while a bare base still works too.
func TestQuoting_AssetRefStillBare(t *testing.T) {
	t.Parallel()

	f, err := Parse("has asset USD/2")
	require.NoError(t, err)
	require.Equal(t, "USD", f.GetAccountHasAsset().GetAssetBase())
	require.Equal(t, uint32(2), f.GetAccountHasAsset().GetPrecision())

	f, err = Parse("has asset USD")
	require.NoError(t, err)
	require.Equal(t, "USD", f.GetAccountHasAsset().GetAssetBase())
	require.Equal(t, uint32(0), f.GetAccountHasAsset().GetPrecision())
}

// TestQuoting_PlainFormsStillBare confirms plain-alphanumeric keys/values are
// still accepted unquoted (no needless quoting required).
func TestQuoting_PlainFormsStillBare(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"metadata[status] == active",
		"metadata[foo_bar] == baz_qux",
		"address == users",
		"ledger == main",
		"metadata[flag] == true",
		"metadata[n] == 42",
		"audit[outcome] == failure",
		// noun words usable as bare values (still lexer keywords, via Value.Kw)
		"metadata[type] == audit",
		"metadata[type] == ledger",
	} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(in)
			require.NoError(t, err, in)
		})
	}
}

// TestQuoting_FormatRoundTripsSpecialChars asserts Format quotes any value that
// isn't a plain bare Ident, so the output reparses to the same filter.
func TestQuoting_FormatRoundTripsSpecialChars(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`metadata["x-request-id"] == "v"`,
		`metadata[k] == "date-2023"`,
		`metadata[k] == "foo.bar"`,
		`address ^= "users:"`,
		`source == "merchants:alice"`,
		`audit[caller_subject] == "svc:payments"`,
		"has asset USD/2",
		// reserved operators as string values must be quoted by Format
		`metadata[k] == "in"`,
		`metadata[k] == "and"`,
	} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			f, err := Parse(in)
			require.NoError(t, err, in)

			out := Format(f)
			reparsed, err := Parse(out)
			require.NoError(t, err, "Format output %q must reparse", out)
			require.True(t, proto.Equal(f, reparsed),
				"round-trip mismatch: in=%q Format=%q", in, out)
		})
	}
}

// structuralKeywords are the words that remain reserved lexer keywords after
// EN-1547 (they must keep terminating expressions). They are the interesting
// stress case for the quote-strings design: a bare punctuated form starting with
// one (metadata[and-x]) is rejected like any punctuated bare identifier, while the
// quoted form is a normal key/value that round-trips.
var structuralKeywords = []string{"and", "or", "not", "in", "between"}

// TestQuoting_StructuralKeywordPrefixes locks the behavior flemzord asked about
// (PR #1582 P2): for each structural keyword, a punctuated key/value that starts
// with it parses only when quoted, the bare form is rejected, and Format quotes
// it so the round-trip holds. Also covers the structural-word-exact value
// (`== "and"`), which must be quoted by Format to reparse as a string.
func TestQuoting_StructuralKeywordPrefixes(t *testing.T) {
	t.Parallel()

	for _, kw := range structuralKeywords {
		for _, sep := range specialChars {
			punct := kw + sep + "x"

			t.Run(punct, func(t *testing.T) {
				t.Parallel()

				// (a) quoted key parses; bare punctuated key rejected.
				quotedKey := fmt.Sprintf(`metadata["%s"] == "v"`, punct)
				fk, err := Parse(quotedKey)
				require.NoError(t, err, quotedKey)
				require.Equal(t, punct, fk.GetField().GetField().GetMetadata(), quotedKey)

				bareKey := fmt.Sprintf(`metadata[%s] == "v"`, punct)
				_, err = Parse(bareKey)
				require.Error(t, err, bareKey)

				// (b) quoted value parses.
				quotedVal := fmt.Sprintf(`metadata[k] == "%s"`, punct)
				fv, err := Parse(quotedVal)
				require.NoError(t, err, quotedVal)
				require.Equal(t, punct, fv.GetField().GetStringCond().GetHardcoded(), quotedVal)

				// (c) Format round-trips the punctuated value (Format quotes it).
				out := Format(fv)
				rp, err := Parse(out)
				require.NoError(t, err, out)
				require.True(t, proto.Equal(fv, rp), "round-trip: value=%q Format=%q", punct, out)
			})
		}

		// (d) The structural-word-exact string value must be quoted by Format so
		// it reparses as a string rather than an operator token.
		t.Run(kw+"_exact_value", func(t *testing.T) {
			t.Parallel()

			f, err := Parse(fmt.Sprintf(`metadata[k] == "%s"`, kw))
			require.NoError(t, err)
			require.Equal(t, kw, f.GetField().GetStringCond().GetHardcoded())

			out := Format(f)
			rp, err := Parse(out)
			require.NoError(t, err, out)
			require.True(t, proto.Equal(f, rp), "exact round-trip: %q Format=%q", kw, out)

			// The bare operator word is NOT a valid value (it terminates the expr).
			_, err = Parse("metadata[k] == " + kw)
			require.Error(t, err)
		})
	}
}
