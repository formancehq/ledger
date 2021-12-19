package validators

import (
	"github.com/go-playground/validator/v10"
	"github.com/numary/ledger/core"
)

// ValidateSourceOrDestination
var ValidateSourceOrDestination validator.Func = func(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return core.IsValidSourceOrDestination(value)
	}
	return false
}

// ValidateAsset
var ValidateAsset validator.Func = func(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return core.AssetIsValid(value)
	}
	return false
}
