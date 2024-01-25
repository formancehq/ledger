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
		{
			name:       "using single dash",
			address:    "-",
			shouldBeOk: true,
		},
		{
			name:       "using dash without alphanum before",
			address:    "-toto",
			shouldBeOk: true,
		},
		{
			name:       "using dash without alphanum after",
			address:    "toto-",
			shouldBeOk: true,
		},
		{
			name:       "using dash",
			address:    "toto-titi",
			shouldBeOk: true,
		},
		{
			name:       "using dash multi segment",
			address:    "toto-titi:tata-tutu",
			shouldBeOk: true,
		},
		{
			name:       "using multiple dashes",
			address:    "toto----titi",
			shouldBeOk: true,
		},
		{
			name:       "using multiple dashes 2",
			address:    "-toto----titi-",
			shouldBeOk: true,
		},
	}

	for _, testCase := range testsCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.shouldBeOk, ValidateAddress(testCase.address))
		})
	}
}
