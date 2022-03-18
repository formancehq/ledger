package bus

import (
	"go.uber.org/fx/fxtest"
	"testing"
)

func TestModuleDefault(t *testing.T) {
	app := fxtest.New(t, Module())
	app.
		RequireStart().
		RequireStop()
}
