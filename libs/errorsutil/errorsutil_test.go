package errorsutil_test

import (
	"errors"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	pkgError "github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var (
	ErrStorageNotFound = errors.New("not found")
	ErrNotFound        = errors.New("not found")
)

func TestError(t *testing.T) {
	basicError := errors.New("got an error")

	wrapError1 := errorsutil.NewError(ErrStorageNotFound, basicError)
	wrapError2 := errorsutil.NewError(ErrNotFound, wrapError1)
	pkgWrapError := pkgError.Wrap(wrapError2, "pkg wrap")

	require.True(t, errors.Is(wrapError2, ErrNotFound))
	require.True(t, errors.Is(wrapError2, ErrStorageNotFound))

	require.True(t, errors.Is(pkgWrapError, ErrNotFound))
	require.True(t, errors.Is(pkgWrapError, ErrStorageNotFound))

	require.Equal(t, pkgError.Cause(wrapError2), basicError)
	require.Equal(t, pkgError.Cause(pkgWrapError), basicError)
}
