package controllers

import (
	"reflect"
	"testing"
)

func TestNewTransactionController(t *testing.T) {
	if reflect.TypeOf(NewTransactionController()) != reflect.TypeOf(&TransactionController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(NewTransactionController()),
			reflect.TypeOf(&TransactionController{}),
		)
	}
}
