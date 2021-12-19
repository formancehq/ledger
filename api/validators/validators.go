package validators

import (
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"go.uber.org/fx"
)

var (
	Module = fx.Options()
)

// Validators struct
type Validators struct{}

// NewValidators
func NewValidators() Validators {
	return Validators{}
}

// Register
func (validators *Validators) Register() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterValidation("source", ValidateSourceOrDestination)
		v.RegisterValidation("destination", ValidateSourceOrDestination)
		v.RegisterValidation("asset", ValidateAsset)
	}
}
