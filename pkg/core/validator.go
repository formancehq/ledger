package core

import (
	"github.com/go-playground/validator/v10"
	"github.com/numary/ledger/pkg/core/global"
)

type Validator struct {
	validator *validator.Validate
}

func NewValidator() Validator {
	return Validator{
		validator: validator.New(),
	}
}

// Register
func (v *Validator) Register() {
	v.validator.RegisterValidation("source", v.validateSourceOrDestination)
	v.validator.RegisterValidation("destination", v.validateSourceOrDestination)
	v.validator.RegisterValidation("asset", v.validateAsset)
}

// Validate
func (v *Validator) Validate(value interface{}) error {
	if err := v.validator.Var(value, "required,dive"); err != nil {
		return err
	}
	return nil
}

func (v *Validator) validateSourceOrDestination(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return global.RegexSourceOrDestinationFormat.MatchString(value)
	}
	return false
}

func (v *Validator) validateAsset(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	if ok {
		return global.RegexAssetFormat.MatchString(value)
	}
	return false
}
