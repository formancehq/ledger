package tests

import (
	"reflect"
	"testing"

	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/api/services"
)

func TestNewConfigController(t *testing.T) {
	newConfigController := controllers.NewConfigController(services.CreateConfigService())
	if reflect.TypeOf(newConfigController) != reflect.TypeOf(&controllers.ConfigController{}) {
		t.Errorf(
			"%s return type is '%s' should be '%s'",
			t.Name(),
			reflect.TypeOf(newConfigController),
			reflect.TypeOf(&controllers.ConfigController{}),
		)
	}
}
