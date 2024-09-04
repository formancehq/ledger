package system

import (
	"github.com/pkg/errors"
)

var (
	ErrNeedUpgradeBucket   = errors.New("need to upgrade bucket before add a new ledger on it")
	ErrLedgerAlreadyExists = errors.New("ledger already exists")
)
