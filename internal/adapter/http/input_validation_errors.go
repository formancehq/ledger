package http

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

var inputValidationErrors = map[inputvalidation.Key]error{
	{Message: "common.SinkConfig", Path: "name", Rule: "non_zero"}: errors.New("sink name and one sink type are required"),
}

func mapInputValidation(err error) error {
	return inputvalidation.Resolve(err, inputValidationErrors)
}
