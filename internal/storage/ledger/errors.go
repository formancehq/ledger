package ledger

import (
	"errors"
)

var (
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrStoreAlreadyExists  = errors.New("store already exists")
	ErrStoreNotFound       = errors.New("store not found")
)
