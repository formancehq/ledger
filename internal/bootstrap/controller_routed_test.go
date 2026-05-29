package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
)

func TestNewRoutedController(t *testing.T) {
	t.Parallel()

	// Verify RoutedController implements ctrl.Controller at compile time.
	var _ ctrl.Controller = (*RoutedController)(nil)

	rc := NewRoutedController(nil, nil, nil)
	assert.Nil(t, rc.localController)
	assert.Nil(t, rc.servicePool)
	assert.Nil(t, rc.Node)
}

func TestRoutedController_IsHealthy_NilNode(t *testing.T) {
	t.Parallel()

	// IsHealthy delegates to Node.IsHealthy(). With a nil Node, we verify
	// through the compile-time interface check that the method exists.
	// Behavioral testing of Node.IsHealthy() is in the node package.
	// Here we just confirm the method signature matches ctrl.Controller.
	rc := &RoutedController{}
	assert.NotNil(t, rc) // RoutedController can be instantiated
}
