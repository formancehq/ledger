package controllers

import (
	"reflect"
	"testing"
)

func TestNewTransactionController(t *testing.T) {
	if reflect.TypeOf(NewTransactionController()) != reflect.TypeOf(&TransactionController{}) {
		t.Errorf(
			"TransactionController return type is '%s' should be '%s'",
			reflect.TypeOf(NewTransactionController()),
			reflect.TypeOf(&TransactionController{}),
		)
	}
}
