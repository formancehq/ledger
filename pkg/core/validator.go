package core

import (
	"regexp"

	"github.com/go-playground/validator/v10"
)

type Validator struct {
	validator  *validator.Validate
	registered bool
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
	v.registered = true
}

// Validate
func (v *Validator) Validate(value interface{}) error {
	if !v.registered {
		v.Register()
	}
	if err := v.validator.Var(value, "required,dive"); err != nil {
		return err
	}
	return nil
}

func (v *Validator) validateSourceOrDestination(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	return ok && regexp.MustCompile(`^[a-zA-Z_0-9]+(:[a-zA-Z_0-9]+){0,}$`).MatchString(value)
}

func (v *Validator) validateAsset(fl validator.FieldLevel) bool {
	value, ok := fl.Field().Interface().(string)
	return ok && regexp.MustCompile(`^[A-Z]{1,16}(\/\d{1,6})$`).MatchString(value)
}
