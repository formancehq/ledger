package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
)

func TestNewAccountController(t *testing.T) {
	if reflect.TypeOf(controllers.NewAccountController()) != reflect.TypeOf(&controllers.AccountController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(controllers.NewAccountController()),
			reflect.TypeOf(&controllers.AccountController{}),
		)
	}
}
