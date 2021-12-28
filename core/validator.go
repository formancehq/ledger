package core

import (
	"github.com/go-playground/validator/v10"
)

func register(validator *validator.Validate) {
	validator.RegisterValidation("source", validateSourceOrDestination)
	validator.RegisterValidation("destination", validateSourceOrDestination)
	validator.RegisterValidation("asset", validateAsset)
}

// Validate
func Validate(value interface{}) error {
	validator := validator.New()
	register(validator)
	if err := validator.Var(value, "required,dive"); err != nil {
		return err
	}
	return nil
}

var validateSourceOrDestination validator.Func = func(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return IsValidSourceOrDestination(value)
	}
	return false
}

var validateAsset validator.Func = func(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return AssetIsValid(value)
	}
	return false
}
