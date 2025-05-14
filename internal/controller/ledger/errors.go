package ledger

import (
	"encoding/base64"
	"fmt"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/numscript"

	"github.com/formancehq/ledger/internal/machine"

	"errors"
)

var ErrNotFound = postgres.ErrNotFound

type ErrTooManyClient = postgres.ErrTooManyClient

type ErrImport struct {
	err error
}

func (i ErrImport) Error() string {
	return i.err.Error()
}

func (i ErrImport) Is(err error) bool {
	_, ok := err.(ErrImport)
	return ok
}

var _ error = (*ErrImport)(nil)

func NewErrImport(err error) ErrImport {
	return ErrImport{
		err: err,
	}
}

var _ error = (*ErrInvalidHash)(nil)

type ErrInvalidHash struct {
	logID    uint64
	expected []byte
	got      []byte
}

func (i ErrInvalidHash) Error() string {
	return fmt.Sprintf(
		"invalid hash, expected %s got %s for log %d",
		base64.StdEncoding.EncodeToString(i.expected),
		base64.StdEncoding.EncodeToString(i.got),
		i.logID,
	)
}

var _ error = (*ErrInvalidHash)(nil)

func newErrInvalidHash(logID uint64, got, expected []byte) ErrImport {
	return NewErrImport(ErrInvalidHash{
		expected: expected,
		got:      got,
		logID:    logID,
	})
}

// todo(waiting): need a more precise underlying error
// notes(gfyrag): Waiting new interpreter
type ErrInsufficientFunds = machine.ErrInsufficientFund

var ErrNoPostings = errors.New("numscript execution returned no postings")

type ErrAlreadyReverted struct {
	id uint64
}

func (e ErrAlreadyReverted) Error() string {
	return fmt.Sprintf("already reverted, id: %d", e.id)
}

func (e ErrAlreadyReverted) Is(err error) bool {
	_, ok := err.(ErrAlreadyReverted)
	return ok
}

var _ error = (*ErrAlreadyReverted)(nil)

func newErrAlreadyReverted(id uint64) ErrAlreadyReverted {
	return ErrAlreadyReverted{
		id: id,
	}
}

type ErrMissingFeature struct {
	feature string
}

func (e ErrMissingFeature) Error() string {
	return fmt.Sprintf("missing feature %q", e.feature)
}

func (e ErrMissingFeature) Is(err error) bool {
	_, ok := err.(ErrMissingFeature)
	return ok
}

func NewErrMissingFeature(feature string) ErrMissingFeature {
	return ErrMissingFeature{
		feature: feature,
	}
}

type ErrIdempotencyKeyConflict struct {
	ik string
}

func (e ErrIdempotencyKeyConflict) Error() string {
	return fmt.Sprintf("duplicate idempotency key %q", e.ik)
}

func (e ErrIdempotencyKeyConflict) Is(err error) bool {
	_, ok := err.(ErrIdempotencyKeyConflict)
	return ok
}

func NewErrIdempotencyKeyConflict(ik string) ErrIdempotencyKeyConflict {
	return ErrIdempotencyKeyConflict{
		ik: ik,
	}
}

type ErrTransactionReferenceConflict struct {
	reference string
}

func (e ErrTransactionReferenceConflict) Error() string {
	return fmt.Sprintf("duplicate reference %q", e.reference)
}

func (e ErrTransactionReferenceConflict) Is(err error) bool {
	_, ok := err.(ErrTransactionReferenceConflict)
	return ok
}

func NewErrTransactionReferenceConflict(reference string) ErrTransactionReferenceConflict {
	return ErrTransactionReferenceConflict{
		reference: reference,
	}
}

type ErrInvalidVars = machine.ErrInvalidVars

// ErrCompilationFailed is used for any errors returned by the numscript interpreter
type ErrCompilationFailed struct {
	err error
}

func (e ErrCompilationFailed) Error() string {
	return fmt.Sprintf("compilation error: %s", e.err)
}

func (e ErrCompilationFailed) Is(err error) bool {
	_, ok := err.(ErrCompilationFailed)
	return ok
}

func newErrCompilationFailed(err error) ErrCompilationFailed {
	return ErrCompilationFailed{
		err: err,
	}
}

type ErrRuntime struct {
	Source string
	Inner  numscript.InterpreterError
}

func (e ErrRuntime) Error() string {
	return e.Inner.Error()
}

func (e ErrRuntime) Is(err error) bool {
	_, ok := err.(ErrRuntime)
	return ok
}

type ErrParsing struct {
	Source string
	// Precondition: Errors is not empty
	Errors []numscript.ParserError
}

func (e ErrParsing) Error() string {
	return numscript.ParseErrorsToString(e.Errors, e.Source)
}

func (e ErrParsing) Is(err error) bool {
	_, ok := err.(ErrParsing)
	return ok
}

// ErrMetadataOverride is used when a metadata is defined at numscript level AND at the input level
type ErrMetadataOverride struct {
	key string
}

func (e *ErrMetadataOverride) Error() string {
	return fmt.Sprintf("cannot override metadata '%s'", e.key)
}

func (e *ErrMetadataOverride) Is(err error) bool {
	_, ok := err.(*ErrMetadataOverride)
	return ok
}

func newErrMetadataOverride(key string) *ErrMetadataOverride {
	return &ErrMetadataOverride{
		key: key,
	}
}

// ErrInvalidIdempotencyInput is used when a IK is used with an inputs different from the original one.
// For example, try to use the same IK with a different numscript script will result with that error.
type ErrInvalidIdempotencyInput struct {
	idempotencyKey          string
	expectedIdempotencyHash string
	computedIdempotencyHash string
}

func (e ErrInvalidIdempotencyInput) Error() string {
	return fmt.Sprintf(
		"invalid idempotency hash when using idempotency key '%s', has computed '%s' but '%s' is stored",
		e.idempotencyKey,
		e.computedIdempotencyHash,
		e.expectedIdempotencyHash,
	)
}

func (e ErrInvalidIdempotencyInput) Is(err error) bool {
	_, ok := err.(ErrInvalidIdempotencyInput)
	if ok {
		return true
	}
	_, ok = err.(*ErrInvalidIdempotencyInput)
	return ok
}

func newErrInvalidIdempotencyInputs(idempotencyKey, expectedIdempotencyHash, gotIdempotencyHash string) ErrInvalidIdempotencyInput {
	return ErrInvalidIdempotencyInput{
		idempotencyKey:          idempotencyKey,
		expectedIdempotencyHash: expectedIdempotencyHash,
		computedIdempotencyHash: gotIdempotencyHash,
	}
}

// ErrConcurrentTransaction can be raised in case of conflicting between an import and a single transaction
type ErrConcurrentTransaction struct {
	id uint64
}

func (e ErrConcurrentTransaction) Error() string {
	return fmt.Sprintf("duplicate id insertion %d", e.id)
}

func (e ErrConcurrentTransaction) Is(err error) bool {
	_, ok := err.(ErrConcurrentTransaction)
	return ok
}

func NewErrConcurrentTransaction(id uint64) ErrConcurrentTransaction {
	return ErrConcurrentTransaction{
		id: id,
	}
}
