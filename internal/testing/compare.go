package testing

import (
	"math/big"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func bigIntComparer(v1 *big.Int, v2 *big.Int) bool {
	return v1.String() == v2.String()
}

func RequireEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, cmp.Comparer(bigIntComparer)); diff != "" {
		require.Failf(t, "Content not matching", diff)
	}
}
