package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
)

func TestNewLedgerController(t *testing.T) {
	if reflect.TypeOf(controllers.NewLedgerController()) != reflect.TypeOf(&controllers.LedgerController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(controllers.NewLedgerController()),
			reflect.TypeOf(&controllers.LedgerController{}),
		)
	}
}
