package commonpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestParseTarget_AccountErrors covers the cases where ParseTarget must
// surface a descriptive error rather than returning a nil Target.
func TestParseTarget_AccountErrors(t *testing.T) {
	t.Parallel()

	t.Run("account requires targetId", func(t *testing.T) {
		t.Parallel()

		_, err := commonpb.ParseTarget("ACCOUNT", nil, "")
		require.ErrorContains(t, err, "requires targetId")
	})

	t.Run("account targetId must be a string", func(t *testing.T) {
		t.Parallel()

		_, err := commonpb.ParseTarget("ACCOUNT", json.RawValue(`42`), "")
		require.ErrorContains(t, err, "string")
	})

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		target, err := commonpb.ParseTarget("ACCOUNT", json.RawValue(`"users:alice"`), "")
		require.NoError(t, err)
		require.Equal(t, "users:alice", target.GetAccount().GetAddr())
	})
}

func TestParseTarget_TransactionErrors(t *testing.T) {
	t.Parallel()

	t.Run("transaction requires id or reference", func(t *testing.T) {
		t.Parallel()

		_, err := commonpb.ParseTarget("TRANSACTION", nil, "")
		require.ErrorContains(t, err, "requires either targetId or targetReference")
	})

	t.Run("transaction rejects both id and reference", func(t *testing.T) {
		t.Parallel()

		_, err := commonpb.ParseTarget("TRANSACTION", json.RawValue(`42`), "invoice:42")
		require.ErrorContains(t, err, "not both")
	})

	t.Run("transaction targetId must be uint64", func(t *testing.T) {
		t.Parallel()

		_, err := commonpb.ParseTarget("TRANSACTION", json.RawValue(`"not-a-number"`), "")
		require.ErrorContains(t, err, "uint64")
	})

	t.Run("happy path by id", func(t *testing.T) {
		t.Parallel()

		target, err := commonpb.ParseTarget("TRANSACTION", json.RawValue(`42`), "")
		require.NoError(t, err)
		require.Equal(t, uint64(42), target.GetTransaction().GetId())
	})

	t.Run("happy path by reference", func(t *testing.T) {
		t.Parallel()

		target, err := commonpb.ParseTarget("TRANSACTION", nil, "invoice:42")
		require.NoError(t, err)
		require.Equal(t, "invoice:42", target.GetTransaction().GetReference())
	})
}

func TestParseTarget_UnsupportedType(t *testing.T) {
	t.Parallel()

	_, err := commonpb.ParseTarget("LEDGER_WIDE", nil, "")
	require.ErrorContains(t, err, "unsupported targetType")
}
