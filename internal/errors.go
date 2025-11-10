package ledger

import (
	"fmt"
)

type ErrInvalidLedgerName struct {
	err  error
	name string
}

func (e ErrInvalidLedgerName) Error() string {
	return fmt.Sprintf("invalid ledger name '%s': %s", e.name, e.err)
}

func (e ErrInvalidLedgerName) Is(err error) bool {
	_, ok := err.(ErrInvalidLedgerName)
	return ok
}

func newErrInvalidLedgerName(name string, err error) ErrInvalidLedgerName {
	return ErrInvalidLedgerName{err: err, name: name}
}

type ErrInvalidBucketName struct {
	err    error
	bucket string
}

func (e ErrInvalidBucketName) Error() string {
	return fmt.Sprintf("invalid bucket name '%s': %s", e.bucket, e.err)
}

func (e ErrInvalidBucketName) Is(err error) bool {
	_, ok := err.(ErrInvalidBucketName)
	return ok
}

func newErrInvalidBucketName(bucket string, err error) ErrInvalidBucketName {
	return ErrInvalidBucketName{err: err, bucket: bucket}
}

// ErrPipelineAlreadyExists denotes a pipeline already created
// The store is in charge of returning this error on a failing call on Store.CreatePipeline
type ErrPipelineAlreadyExists PipelineConfiguration

func (e ErrPipelineAlreadyExists) Error() string {
	return fmt.Sprintf("pipeline '%s/%s' already exists", e.Ledger, e.ExporterID)
}

func (e ErrPipelineAlreadyExists) Is(err error) bool {
	_, ok := err.(ErrPipelineAlreadyExists)
	return ok
}

func NewErrPipelineAlreadyExists(pipelineConfiguration PipelineConfiguration) ErrPipelineAlreadyExists {
	return ErrPipelineAlreadyExists(pipelineConfiguration)
}

type ErrPipelineNotFound string

func (e ErrPipelineNotFound) Error() string {
	return fmt.Sprintf("pipeline '%s' not found", string(e))
}

func (e ErrPipelineNotFound) Is(err error) bool {
	_, ok := err.(ErrPipelineNotFound)
	return ok
}

func NewErrPipelineNotFound(id string) ErrPipelineNotFound {
	return ErrPipelineNotFound(id)
}

type ErrAlreadyStarted string

func (e ErrAlreadyStarted) Error() string {
	return fmt.Sprintf("pipeline '%s' already started", string(e))
}

func (e ErrAlreadyStarted) Is(err error) bool {
	_, ok := err.(ErrAlreadyStarted)
	return ok
}

func NewErrAlreadyStarted(id string) ErrAlreadyStarted {
	return ErrAlreadyStarted(id)
}

type ErrInvalidAccount struct {
	path    []string
	segment string
}

func (e ErrInvalidAccount) Error() string {
	return fmt.Sprintf("segment `%v` is not allowed by the chart of accounts at `%v`", e.segment, e.path)
}
func (e ErrInvalidAccount) Is(err error) bool {
	_, ok := err.(ErrInvalidAccount)
	return ok
}

type ErrDestinationNotAllowed struct {
	source              string
	destination         string
	allowedDestinations []string
}

func (e ErrDestinationNotAllowed) Error() string {
	return fmt.Sprintf("account `%v` cannot send to account `%v` (allowed destinations: %v)", e.source, e.destination, e.allowedDestinations)
}
func (e ErrDestinationNotAllowed) Is(err error) bool {
	_, ok := err.(ErrDestinationNotAllowed)
	return ok
}

type ErrSourceNotAllowed struct {
	source         string
	destination    string
	allowedSources []string
}

func (e ErrSourceNotAllowed) Error() string {
	return fmt.Sprintf("account `%v` cannot receive from account `%v` (allowed sources: %v)", e.destination, e.source, e.allowedSources)
}
func (e ErrSourceNotAllowed) Is(err error) bool {
	_, ok := err.(ErrSourceNotAllowed)
	return ok
}
