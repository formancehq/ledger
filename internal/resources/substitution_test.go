package resources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoInterp(t *testing.T) {
	strs, vars, err := ParseTemplate("abc")

	require.NoError(t, err)
	require.Nil(t, vars)
	require.Equal(t, []string{"abc"}, strs)
}

func TestSimpleInterp(t *testing.T) {
	strs, vars, err := ParseTemplate("abc<xy>")

	require.NoError(t, err)
	require.Nil(t, err)
	require.Equal(t, []string{"abc"}, strs)
	require.Equal(t, []string{"xy"}, vars)
}

func TestManyInterp(t *testing.T) {
	strs, vars, err := ParseTemplate("abc<xy>d<z>")

	require.NoError(t, err)
	require.Nil(t, err)
	require.Equal(t, []string{"abc", "d"}, strs)
	require.Equal(t, []string{"xy", "z"}, vars)
}

func TestManyInterpNoSpaceBetween(t *testing.T) {
	strs, vars, err := ParseTemplate("abc<xy><z>")

	require.NoError(t, err)
	require.Nil(t, err)
	require.Equal(t, []string{"abc", ""}, strs) // <- TODO are we ok with empty str?
	require.Equal(t, []string{"xy", "z"}, vars)
}

func TestOnlyInterp(t *testing.T) {
	strs, vars, err := ParseTemplate("<x>")

	require.NoError(t, err)
	require.Nil(t, err)
	require.Equal(t, []string{""}, strs) // <- TODO are we ok with empty str?
	require.Equal(t, []string{"x"}, vars)
}

func TestAllowEveryChar(t *testing.T) {
	strs, vars, err := ParseTemplate("!@?\\\n<x>")

	require.NoError(t, err)
	require.Nil(t, err)
	require.Equal(t, []string{"!@?\\\n"}, strs)
	require.Equal(t, []string{"x"}, vars)
}

func TestRejectNestedInterp(t *testing.T) {
	_, _, err := ParseTemplate("abc<<xy>")
	require.Error(t, err)
}

func TestRejectSpaces(t *testing.T) {
	_, _, err := ParseTemplate("abc<xy z>")
	require.Error(t, err)
}

func TestRejectNumbers(t *testing.T) {
	_, _, err := ParseTemplate("abc<42>")
	require.Error(t, err)
}
