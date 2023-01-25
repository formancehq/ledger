package storage

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("not found")

func e(msg string, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", msg, ErrNotFound)
	}

	return fmt.Errorf("%s: %w", msg, err)
}
