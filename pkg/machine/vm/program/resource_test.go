package program

import (
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestResource(t *testing.T) {
	c := Constant{
		Inner: core.NewMonetaryInt(0),
	}
	c.GetType()
	require.Equal(t, "0", c.String())

	v := Variable{
		Typ:  core.TypeAccount,
		Name: "acc",
	}
	require.Equal(t, "<account acc>", v.String())

	vab := VariableAccountBalance{
		Name:    "name",
		Account: core.Address(0),
		Asset:   core.Address(1),
	}
	require.Equal(t, "<monetary name balance(0, 1)>", vab.String())

	vam := VariableAccountMetadata{
		Typ:     core.TypeMonetary,
		Name:    "name",
		Account: core.Address(0),
		Key:     "key",
	}
	require.Equal(t, "<monetary name meta(0, key)>", vam.String())
}
