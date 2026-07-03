package v2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func rule(pattern, replacement string) *commonpb.AddressRewriteRule {
	return &commonpb.AddressRewriteRule{Pattern: pattern, Replacement: replacement}
}

func TestNewAddressRewriter(t *testing.T) {
	t.Parallel()

	t.Run("no rules yields a nil pass-through", func(t *testing.T) {
		t.Parallel()

		r, err := NewAddressRewriter(nil)
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("invalid pattern errors", func(t *testing.T) {
		t.Parallel()

		_, err := NewAddressRewriter([]*commonpb.AddressRewriteRule{rule("(", "")})
		require.Error(t, err)
	})

	t.Run("empty pattern errors", func(t *testing.T) {
		t.Parallel()

		_, err := NewAddressRewriter([]*commonpb.AddressRewriteRule{rule("", "x")})
		require.Error(t, err)
	})
}

func TestAddressRewriter_Rewrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []*commonpb.AddressRewriteRule
		in      string
		want    string
		wantErr bool
	}{
		{
			name:  "nil receiver passes through",
			rules: nil,
			in:    "payments:acme:worker:001:main",
			want:  "payments:acme:worker:001:main",
		},
		{
			name:  "drop worker shard segment",
			rules: []*commonpb.AddressRewriteRule{rule(`(:worker:\d+)`, "")},
			in:    "payments:acme:worker:001:main",
			want:  "payments:acme:main",
		},
		{
			name:  "rename prefix",
			rules: []*commonpb.AddressRewriteRule{rule(`^payments:`, "psp:")},
			in:    "payments:acme:main",
			want:  "psp:acme:main",
		},
		{
			name:  "no match leaves address unchanged",
			rules: []*commonpb.AddressRewriteRule{rule(`(:worker:\d+)`, "")},
			in:    "world",
			want:  "world",
		},
		{
			name: "rules apply in order",
			rules: []*commonpb.AddressRewriteRule{
				rule(`(:worker:\d+)`, ""),
				rule(`^payments:`, "psp:"),
			},
			in:   "payments:acme:worker:001:main",
			want: "psp:acme:main",
		},
		{
			name:    "rewrite producing an invalid address errors",
			rules:   []*commonpb.AddressRewriteRule{rule(`.+`, "")},
			in:      "payments:acme:main",
			wantErr: true,
		},
		{
			name:    "rewrite producing an empty segment errors",
			rules:   []*commonpb.AddressRewriteRule{rule(`acme`, "")},
			in:      "payments:acme:main",
			wantErr: true, // -> "payments::main" (empty segment)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, err := NewAddressRewriter(tc.rules)
			require.NoError(t, err)

			got, err := r.Rewrite(tc.in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
