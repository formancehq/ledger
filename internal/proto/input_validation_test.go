package proto_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestGeneratedInputErrorIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source, destination, path string
	}{
		{name: "source", path: "source"},
		{name: "destination", source: "users:source", path: "destination"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := (&commonpb.Posting{Source: tc.source, Destination: tc.destination}).ValidateInput()
			var typed *proto.AccountAddressEmpty
			require.ErrorAs(t, err, &typed)
			var field *proto.FieldError
			require.ErrorAs(t, err, &field)
			require.Equal(t, tc.path, field.Path)
			require.Equal(t, "ACCOUNT_ADDRESS_EMPTY", field.ValidationCode())
			require.ErrorIs(t, err, domain.ErrAccountAddressEmpty)
			require.False(t, errors.Is(err, domain.ErrAssetInvalid))
			message, metadata, public := domain.PublicErrorDetails(err)
			require.True(t, public)
			require.Equal(t, "ledger account address must not be empty", message)
			require.Empty(t, metadata)
		})
	}
}
