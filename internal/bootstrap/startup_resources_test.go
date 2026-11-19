package bootstrap

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartupResourcesCloseAllAfterPartialFxRollback(t *testing.T) {
	resources := NewStartupResources()
	first, second := new(int), new(int)
	var order []int
	resources.Track(first, func() error {
		order = append(order, 1)

		return nil
	})
	resources.Track(second, func() error {
		order = append(order, 2)

		return nil
	})

	require.NoError(t, resources.Close(second)) // Fx already stopped this resource.
	require.NoError(t, resources.CloseAll())
	require.NoError(t, resources.CloseAll())
	require.Equal(t, []int{2, 1}, order)
}

func TestStartupResourcesPreservesCloseError(t *testing.T) {
	resources := NewStartupResources()
	resource := new(int)
	want := errors.New("close failed")
	resources.Track(resource, func() error { return want })
	require.ErrorIs(t, resources.CloseAll(), want)
}
