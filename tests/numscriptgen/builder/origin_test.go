package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOriginVarDeclaredInEveryBuild(t *testing.T) {
	t.Parallel()

	stmt := StmtSend(
		NewMonetaryVarFromBalance(ExprAccount("a"), ExprAsset("USD")),
		SrcAccount(ExprAccount("a")),
		DestAccount(ExprAccount("b")),
	)

	_, _, first := BuildProgram(stmt)
	_, _, second := BuildProgram(stmt)

	require.Contains(t, first, "$originvar_0 = balance(")
	require.Equal(t, first, second)
}
