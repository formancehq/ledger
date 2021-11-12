package controllers

import (
	"reflect"
	"testing"
)

func TestNewAccountController(t *testing.T) {
	if reflect.TypeOf(NewAccountController()) != reflect.TypeOf(&AccountController{}) {
		t.Errorf(
			"NewAccountController return type is '%s' should be '%s'",
			reflect.TypeOf(NewAccountController()),
			reflect.TypeOf(&AccountController{}),
		)
	}
}
