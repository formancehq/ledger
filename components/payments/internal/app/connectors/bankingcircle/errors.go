package bankingcircle

import "github.com/pkg/errors"

var (
	// ErrMissingTask is returned when the task is missing.
	ErrMissingTask = errors.New("task is not implemented")

	// ErrMissingUsername is returned when the username is missing.
	ErrMissingUsername = errors.New("missing username from config")

	// ErrMissingPassword is returned when the password is missing.
	ErrMissingPassword = errors.New("missing password from config")

	// ErrMissingEndpoint is returned when the endpoint is missing.
	ErrMissingEndpoint = errors.New("missing endpoint from config")

	// ErrMissingAuthorizationEndpoint is returned when the authorization endpoint is missing.
	ErrMissingAuthorizationEndpoint = errors.New("missing authorization endpoint from config")
)
