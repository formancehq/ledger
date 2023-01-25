package formance

import (
	"github.com/pkg/errors"
)

func UnwrapOpenAPIError(err error) *ErrorResponse {
	for err != nil {
		if err, ok := err.(*GenericOpenAPIError); ok {
			model := err.Model()
			if errorResponse, ok := model.(ErrorResponse); ok {
				return &errorResponse
			}
		}

		err = errors.Unwrap(err)
	}
	return nil
}

func ExtractOpenAPIErrorMessage(err error) error {
	if err := UnwrapOpenAPIError(err); err != nil {
		return errors.New(err.GetErrorMessage())
	}
	return err
}
