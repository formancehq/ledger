package controllers

import (
	"reflect"
	"testing"
)

func TestNewAccountController(t *testing.T) {
	if reflect.TypeOf(NewAccountController()) != reflect.TypeOf(&AccountController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(NewAccountController()),
			reflect.TypeOf(&AccountController{}),
		)
	}
}
