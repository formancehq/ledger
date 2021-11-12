package controllers

import (
	"reflect"
	"testing"
)

func TestNewBaseController(t *testing.T) {
	if reflect.TypeOf(NewBaseController()) != reflect.TypeOf(&BaseController{}) {
		t.Errorf(
			"NewBaseController return type is '%s' should be '%s'",
			reflect.TypeOf(NewBaseController()),
			reflect.TypeOf(&BaseController{}),
		)
	}
}
