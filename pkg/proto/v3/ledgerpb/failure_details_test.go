package ledgerpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRequiredFailureFactsRejectsInvalidUTF8FromTypedProducer(t *testing.T) {
	t.Parallel()
	name := string([]byte{0xff})
	details := &FailureDetails{Kind: &FailureDetails_AccountTypeNotFound{
		AccountTypeNotFound: &AccountTypeNotFoundDetails{Name: &name},
	}}
	require.ErrorContains(t, ValidateRequiredFailureFacts(details), "UTF-8")
	require.ErrorContains(t, func() error {
		_, err := NewFailureDetails("ACCOUNT_TYPE_NOT_FOUND", map[string]string{"name": name})

		return err
	}(), "UTF-8")
}
