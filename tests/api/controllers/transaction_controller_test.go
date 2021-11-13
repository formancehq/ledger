package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
)

func TestNewTransactionController(t *testing.T) {
	if reflect.TypeOf(controllers.NewTransactionController()) != reflect.TypeOf(&controllers.TransactionController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(controllers.NewTransactionController()),
			reflect.TypeOf(&controllers.TransactionController{}),
		)
	}
}
