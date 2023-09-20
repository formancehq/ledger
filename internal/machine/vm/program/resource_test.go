package program

import (
	"testing"

	internal2 "github.com/formancehq/ledger/internal/machine/internal"
	"github.com/stretchr/testify/require"
)

func TestResource(t *testing.T) {
	c := Constant{
		Inner: internal2.NewMonetaryInt(0),
	}
	c.GetType()
	require.Equal(t, "0", c.String())

	v := Variable{
		Typ:  internal2.TypeAccount,
		Name: "acc",
	}
	require.Equal(t, "<account acc>", v.String())

	vab := VariableAccountBalance{
		Name:    "name",
		Account: internal2.Address(0),
		Asset:   internal2.Address(1),
	}
	require.Equal(t, "<monetary name balance(0, 1)>", vab.String())

	vam := VariableAccountMetadata{
		Typ:     internal2.TypeMonetary,
		Name:    "name",
		Account: internal2.Address(0),
		Key:     "key",
	}
	require.Equal(t, "<monetary name meta(0, key)>", vam.String())
}
