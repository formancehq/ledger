package ledger

import (
	"encoding/base64"
	"fmt"

	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/ledger/internal/machine"

	"github.com/pkg/errors"
)

var ErrNotFound = postgres.ErrNotFound

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

func newErrImport(err error) ErrImport {
	return ErrImport{
		err: err,
	}
}

type ErrInvalidState struct {
	expectedState string
	actualState   string
}

func (i ErrInvalidState) Error() string {
	return fmt.Sprintf(
		"invalid state, expected '%s' got '%s'",
		i.expectedState,
		i.actualState,
	)
}

var _ error = (*ErrInvalidHash)(nil)

func newErrInvalidState(expected, actual string) ErrImport {
	return newErrImport(ErrInvalidState{
		expectedState: expected,
		actualState:   actual,
	})
}

type ErrInvalidHash struct {
	logID    int
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

func newErrInvalidHash(logID int, got, expected []byte) ErrImport {
	return newErrImport(ErrInvalidHash{
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
	id int
}

func (e ErrAlreadyReverted) Error() string {
	return fmt.Sprintf("already reverted, id: %d", e.id)
}

func (e ErrAlreadyReverted) Is(err error) bool {
	_, ok := err.(ErrAlreadyReverted)
	return ok
}

var _ error = (*ErrAlreadyReverted)(nil)

func newErrAlreadyReverted(id int) ErrAlreadyReverted {
	return ErrAlreadyReverted{
		id: id,
	}
}

type ErrInvalidQuery struct {
	msg string
}

func (e ErrInvalidQuery) Error() string {
	return e.msg
}

func (e ErrInvalidQuery) Is(err error) bool {
	_, ok := err.(ErrInvalidQuery)
	return ok
}

func NewErrInvalidQuery(msg string, args ...any) ErrInvalidQuery {
	return ErrInvalidQuery{
		msg: fmt.Sprintf(msg, args...),
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

type ErrReferenceConflict struct {
	reference string
}

func (e ErrReferenceConflict) Error() string {
	return fmt.Sprintf("duplicate reference %q", e.reference)
}

func (e ErrReferenceConflict) Is(err error) bool {
	_, ok := err.(ErrReferenceConflict)
	return ok
}

func NewErrReferenceConflict(reference string) ErrReferenceConflict {
	return ErrReferenceConflict{
		reference: reference,
	}
}

type ErrInvalidVars = machine.ErrInvalidVars

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
