package controllers

import (
	"reflect"
	"testing"
)

func TestNewScriptController(t *testing.T) {
	if reflect.TypeOf(NewScriptController()) != reflect.TypeOf(&ScriptController{}) {
		t.Errorf(
			"ScriptController return type is '%s' should be '%s'",
			reflect.TypeOf(NewScriptController()),
			reflect.TypeOf(&ScriptController{}),
		)
	}
}
