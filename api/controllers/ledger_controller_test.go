package controllers

import (
	"reflect"
	"testing"
)

func TestNewLedgerController(t *testing.T) {
	if reflect.TypeOf(NewLedgerController()) != reflect.TypeOf(&LedgerController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(NewLedgerController()),
			reflect.TypeOf(&LedgerController{}),
		)
	}
}
