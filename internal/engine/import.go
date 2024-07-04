package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
)

type ImportError struct {
	err   error
	logID *big.Int
}

func (i ImportError) Error() string {
	return i.err.Error()
}

func (i ImportError) Is(err error) bool {
	_, ok := err.(ImportError)
	return ok
}

var _ error = (*ImportError)(nil)

func newImportError(logID *big.Int, err error) ImportError {
	return ImportError{
		logID: logID,
		err:   err,
	}
}

type InvalidIdError struct {
	Expected *big.Int
	Got      *big.Int
}

func (i InvalidIdError) Error() string {
	return fmt.Sprintf("invalid id, got %s, expected %s", i.Got, i.Expected)
}

func (i InvalidIdError) Is(err error) bool {
	_, ok := err.(InvalidIdError)
	return ok
}

var _ error = (*InvalidIdError)(nil)

func newInvalidIdError(got, expected *big.Int) ImportError {
	return newImportError(got, InvalidIdError{
		Expected: expected,
		Got:      got,
	})
}

type InvalidHashError struct {
	Expected []byte
	Got      []byte
}

func (i InvalidHashError) Error() string {
	return fmt.Sprintf(
		"invalid hash, expected %s got %s",
		base64.StdEncoding.EncodeToString(i.Expected),
		base64.StdEncoding.EncodeToString(i.Got),
	)
}

func (i InvalidHashError) Is(err error) bool {
	_, ok := err.(InvalidHashError)
	return ok
}

var _ error = (*InvalidHashError)(nil)

func newInvalidHashError(logID *big.Int, got, expected []byte) ImportError {
	return newImportError(logID, InvalidHashError{
		Expected: expected,
		Got:      got,
	})
}

func (l *Ledger) Import(ctx context.Context, stream chan *ledger.ChainedLog) error {
	if l.config.LedgerState.State != "initializing" {
		return errors.New("ledger must be in initializing state to be imported")
	}
	batch := make([]*ledger.ChainedLog, 0)
	for log := range stream {
		lastLog := l.chain.GetLastLog()
		nextLogID := big.NewInt(0)
		if lastLog != nil {
			nextLogID = nextLogID.Add(lastLog.ID, big.NewInt(1))
		}
		if log.ID.String() != nextLogID.String() {
			return newInvalidIdError(log.ID, nextLogID)
		}
		logHash := log.Hash
		log.Hash = nil
		log.ID = big.NewInt(0)
		log.ComputeHash(lastLog)

		if !reflect.DeepEqual(log.Hash, logHash) {
			return newInvalidHashError(log.ID, log.Hash, logHash)
		}

		log.ID = nextLogID
		l.chain.ReplaceLast(log)

		batch = append(batch, log)
		if len(batch) == 100 { // notes(gfyrag): maybe we could parameterize that, but i don't think it will be useful
			if err := l.store.InsertLogs(ctx, batch...); err != nil {
				return err
			}
			batch = make([]*ledger.ChainedLog, 0)
		}
	}
	if len(batch) > 0 {
		if err := l.store.InsertLogs(ctx, batch...); err != nil {
			return err
		}
	}

	return nil
}
