package controllers

import (
	"reflect"
	"testing"
)

func TestNewScriptController(t *testing.T) {
	if reflect.TypeOf(NewScriptController()) != reflect.TypeOf(&ScriptController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(NewScriptController()),
			reflect.TypeOf(&ScriptController{}),
		)
	}
}
