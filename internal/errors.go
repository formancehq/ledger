package ledger

import "fmt"

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
