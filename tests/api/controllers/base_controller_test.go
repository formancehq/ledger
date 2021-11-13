package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
)

func TestNewBaseController(t *testing.T) {
	if reflect.TypeOf(controllers.NewBaseController()) != reflect.TypeOf(&controllers.BaseController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(controllers.NewBaseController()),
			reflect.TypeOf(&controllers.BaseController{}),
		)
	}
}
