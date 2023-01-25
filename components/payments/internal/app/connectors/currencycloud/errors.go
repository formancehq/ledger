package currencycloud

import "github.com/pkg/errors"

var (
	// ErrMissingTask is returned when the task is missing.
	ErrMissingTask = errors.New("task is not implemented")

	// ErrMissingAPIKey is returned when the api key is missing from config.
	ErrMissingAPIKey = errors.New("missing apiKey from config")

	// ErrMissingLoginID is returned when the login id is missing from config.
	ErrMissingLoginID = errors.New("missing loginID from config")

	// ErrMissingPollingPeriod is returned when the polling period is missing from config.
	ErrMissingPollingPeriod = errors.New("missing pollingPeriod from config")

	// ErrDurationInvalid is returned when the duration is invalid.
	ErrDurationInvalid = errors.New("duration is invalid")
)
