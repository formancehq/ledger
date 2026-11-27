package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

func TestInputValidationPreservesPublicErrorAndSentinel(t *testing.T) {
	t.Parallel()
	err := InputValidation((&commonpb.Posting{}).ValidateInput())
	field := new(inputvalidation.FieldError)
	require.ErrorAs(t, err, &field)
	require.Equal(t, "source", field.Path)
	require.ErrorIs(t, err, ErrAccountAddressEmpty)
	require.Equal(t, KindValidation, err.Kind())
	require.Equal(t, ErrReasonValidation, err.Reason())
	message, metadata, public := PublicErrorDetails(err)
	require.True(t, public)
	require.Equal(t, "ledger account address must not be empty", message)
	require.Empty(t, metadata)
	// A different validation sentinel must not match.
	require.NotErrorIs(t, err, ErrAssetInvalid)
}

func TestInputValidationRejectsIncompleteGeneratedErrors(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { _ = InputValidation(nil) })
	require.Panics(t, func() { _ = InputValidation(&inputvalidation.FieldError{Path: "source"}) })
}
