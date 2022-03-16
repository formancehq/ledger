package bus

import (
	"go.uber.org/fx/fxtest"
	"testing"
)

func TestModule(t *testing.T) {
	app := fxtest.New(t, Module())
	app.
		RequireStart().
		RequireStop()
}
