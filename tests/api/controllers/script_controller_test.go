package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
)

func TestNewScriptController(t *testing.T) {
	if reflect.TypeOf(controllers.NewScriptController()) != reflect.TypeOf(&controllers.ScriptController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(controllers.NewScriptController()),
			reflect.TypeOf(&controllers.ScriptController{}),
		)
	}
}
