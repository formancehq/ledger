package publish

import (
	"testing"

	"go.uber.org/fx/fxtest"
)

func TestModuleDefault(t *testing.T) {
	app := fxtest.New(t, Module())
	app.
		RequireStart().
		RequireStop()
}
