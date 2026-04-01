package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPanickedError_ErrorWithStringPanic(t *testing.T) {
	t.Parallel()

	stack := []byte("goroutine 1 [running]:\nmain.main()\n\t/app/main.go:10")
	pe := newPanickedError("something went wrong", stack)

	msg := pe.Error()
	require.Contains(t, msg, "panic: something went wrong")
	require.Contains(t, msg, "goroutine 1 [running]")
}

func TestPanickedError_ErrorWithErrorPanic(t *testing.T) {
	t.Parallel()

	inner := errors.New("inner error")
	stack := []byte("stack trace")
	pe := newPanickedError(inner, stack)

	msg := pe.Error()
	require.Contains(t, msg, "panic: inner error")
	require.Contains(t, msg, "stack trace")
}

func TestPanickedError_UnwrapWithError(t *testing.T) {
	t.Parallel()

	inner := errors.New("inner error")
	pe := newPanickedError(inner, []byte("stack"))

	unwrapped := pe.Unwrap()
	require.Equal(t, inner, unwrapped)
	require.True(t, errors.Is(pe, inner))
}

func TestPanickedError_UnwrapWithNonError(t *testing.T) {
	t.Parallel()

	pe := newPanickedError("just a string", []byte("stack"))

	unwrapped := pe.Unwrap()
	require.Nil(t, unwrapped)
}

func TestPanickedError_UnwrapWithIntPanic(t *testing.T) {
	t.Parallel()

	pe := newPanickedError(42, []byte("stack"))

	unwrapped := pe.Unwrap()
	require.Nil(t, unwrapped)
}

func TestNewPanickedError(t *testing.T) {
	t.Parallel()

	stack := []byte("test stack")
	pe := newPanickedError("panic value", stack)

	require.NotNil(t, pe)
	require.Equal(t, "panic value", pe.e)
	require.Equal(t, stack, pe.stack)
}
