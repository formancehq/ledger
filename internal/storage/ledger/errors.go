package ledger

import "fmt"

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
