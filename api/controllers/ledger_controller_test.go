package controllers

import (
	"reflect"
	"testing"
)

func TestNewLedgerController(t *testing.T) {
	if reflect.TypeOf(NewLedgerController()) != reflect.TypeOf(&LedgerController{}) {
		t.Errorf(
			"LedgerController return type is %s, should be %s",
			reflect.TypeOf(NewLedgerController()),
			reflect.TypeOf(&LedgerController{}),
		)
	}
}
