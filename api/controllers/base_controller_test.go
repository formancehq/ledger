package controllers

import (
	"reflect"
	"testing"
)

func TestNewBaseController(t *testing.T) {
	if reflect.TypeOf(NewBaseController()) != reflect.TypeOf(&BaseController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(NewBaseController()),
			reflect.TypeOf(&BaseController{}),
		)
	}
}
