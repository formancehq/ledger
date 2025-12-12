package ledger

import "errors"

var (
	ErrNotFound = errors.New("not found")
	ErrNoLeader = errors.New("no leader")
)
