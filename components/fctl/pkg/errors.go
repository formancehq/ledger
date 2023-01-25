package fctl

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

func WrapError(err error, msg string) error {
	if err == nil {
		return nil
	}
	type withBodyError interface {
		Body() []byte
	}
	type errorBody struct {
		ErrorCode    string `json:"error_code"`
		ErrorMessage string `json:"error_message"`
	}
	returnError := err
	switch err := err.(type) {
	case withBodyError:
		b := errorBody{}
		if err := json.Unmarshal(err.Body(), &b); err != nil {
			panic(err)
		}

		returnError = fmt.Errorf("%s: %s", b.ErrorCode, b.ErrorMessage)
	}
	return errors.Wrap(returnError, msg)
}
