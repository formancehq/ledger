package dummypay

import "github.com/pkg/errors"

var (
	// ErrMissingDirectory is returned when the directory is missing.
	ErrMissingDirectory = errors.New("missing directory to watch")

	// ErrFilePollingPeriodInvalid is returned when the file polling period is invalid.
	ErrFilePollingPeriodInvalid = errors.New("file polling period is invalid")

	// ErrFileGenerationPeriodInvalid is returned when the file generation period is invalid.
	ErrFileGenerationPeriodInvalid = errors.New("file generation period is invalid")

	// ErrMissingTask is returned when the task is missing.
	ErrMissingTask = errors.New("task is not implemented")

	// ErrDurationInvalid is returned when the duration is invalid.
	ErrDurationInvalid = errors.New("duration is invalid")
)
