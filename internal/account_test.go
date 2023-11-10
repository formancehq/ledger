package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAddress(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		address    string
		shouldBeOk bool
	}

	testsCases := []testCase{
		{
			name:       "nominal",
			address:    "foo:bar",
			shouldBeOk: true,
		},
		{
			name:       "short segment",
			address:    "a:b",
			shouldBeOk: true,
		},
		{
			name:       "only one segment",
			address:    "a",
			shouldBeOk: true,
		},
		{
			name:       "using underscore as first char",
			address:    "_a",
			shouldBeOk: true,
		},
		{
			name:       "using digits",
			address:    "_0123",
			shouldBeOk: true,
		},
		{
			name:       "using empty segment",
			address:    "a:",
			shouldBeOk: false,
		},
	}

	for _, testCase := range testsCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.shouldBeOk, ValidateAddress(testCase.address))
		})
	}
}
