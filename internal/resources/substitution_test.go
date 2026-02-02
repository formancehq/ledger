package resources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoInterp(t *testing.T) {
	strs, vars, err := ParseTemplate("abc")

	require.Nil(t, err)
	require.Nil(t, vars)
	require.Equal(t, []string{"abc"}, strs)
}

func TestSimpleInterpSyntax(t *testing.T) {
	t.Run("simple interp", func(t *testing.T) {
		strs, vars, err := ParseTemplate("abc$xy")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"abc"}, strs)
		require.Equal(t, []string{"xy"}, vars)
	})

	t.Run("many interp (sep by space)", func(t *testing.T) {
		strs, vars, err := ParseTemplate("abc$xy d$z")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"abc", " d"}, strs)
		require.Equal(t, []string{"xy", "z"}, vars)
	})

	t.Run("many interp (sep by colon)", func(t *testing.T) {
		strs, vars, err := ParseTemplate("abc$xy:d$z")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"abc", ":d"}, strs)
		require.Equal(t, []string{"xy", "z"}, vars)
	})

	t.Run("many interp (no space between)", func(t *testing.T) {
		strs, vars, err := ParseTemplate("abc$xy$z")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"abc", ""}, strs) // <- TODO are we ok with empty str?
		require.Equal(t, []string{"xy", "z"}, vars)
	})

	t.Run("single interp", func(t *testing.T) {
		strs, vars, err := ParseTemplate("$x")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{""}, strs) // <- TODO are we ok with empty str?
		require.Equal(t, []string{"x"}, vars)
	})

	t.Run("allow every char", func(t *testing.T) {
		strs, vars, err := ParseTemplate("!@?\\\n$x")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"!@?\\\n"}, strs)
		require.Equal(t, []string{"x"}, vars)
	})

}

func TestComplexInterp(t *testing.T) {
	t.Run("parse complex interp", func(t *testing.T) {
		strs, vars, err := ParseTemplate("abc${myvar}def")

		require.Nil(t, err)
		require.Nil(t, err)
		require.Equal(t, []string{"abc", "def"}, strs)
		require.Equal(t, []string{"myvar"}, vars)

	})

	t.Run("reject nested interp", func(t *testing.T) {
		_, _, err := ParseTemplate("abc${$}")
		require.Error(t, err)
	})

	t.Run("reject spaces", func(t *testing.T) {
		_, _, err := ParseTemplate("abc${xy z}")
		require.Error(t, err)
	})

	t.Run("reject nums", func(t *testing.T) {
		_, _, err := ParseTemplate("abc${42}")
		require.Error(t, err)
	})
}

func TestErrMsg(t *testing.T) {
	// TODO this should be a snapshot test
	t.Run("unexpected EOF", func(t *testing.T) {
		_, _, err := ParseTemplate("abc${")

		require.Error(t, err)
		require.Equal(t, "i was expecting a lowercase char, but I got EOF instead", err.Error())
	})

	t.Run("missing closing bracket", func(t *testing.T) {
		_, _, err := ParseTemplate("abc${a!def")

		require.Error(t, err)
		require.Equal(t, "i was expecting '}', but I got '!' instead", err.Error())
	})

	t.Run("invalid var head char", func(t *testing.T) {
		_, _, err := ParseTemplate("abc$2")

		require.Error(t, err)
		require.Equal(t, "i was expecting a lowercase char, but I got '2' instead", err.Error())
	})

	t.Run("invalid var c", func(t *testing.T) {
		_, _, err := ParseTemplate("abc$2")

		require.Error(t, err)
		require.Equal(t, "i was expecting a lowercase char, but I got '2' instead", err.Error())
	})

}
