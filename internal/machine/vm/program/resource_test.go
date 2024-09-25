package program

import (
	"testing"

	"github.com/formancehq/ledger/v2/internal/machine"

	"github.com/stretchr/testify/require"
)

func TestResource(t *testing.T) {
	c := Constant{
		Inner: machine.NewMonetaryInt(0),
	}
	c.GetType()
	require.Equal(t, "0", c.String())

	v := Variable{
		Typ:  machine.TypeAccount,
		Name: "acc",
	}
	require.Equal(t, "<account acc>", v.String())

	vab := VariableAccountBalance{
		Name:    "name",
		Account: machine.Address(0),
		Asset:   machine.Address(1),
	}
	require.Equal(t, "<monetary name balance(0, 1)>", vab.String())

	vam := VariableAccountMetadata{
		Typ:     machine.TypeMonetary,
		Name:    "name",
		Account: machine.Address(0),
		Key:     "key",
	}
	require.Equal(t, "<monetary name meta(0, key)>", vam.String())
}
