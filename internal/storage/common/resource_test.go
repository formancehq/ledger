package common_test

import (
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/internal/storage/common"
)

func TestNormalizeDateFilterValue(t *testing.T) {
	t.Parallel()

	t.Run("equivalent instants in different timezones normalize identically", func(t *testing.T) {
		t.Parallel()

		withOffset, err := common.NormalizeDateFilterValue("2026-05-21T15:09:13+04:00")
		require.NoError(t, err)

		withUTC, err := common.NormalizeDateFilterValue("2026-05-21T11:09:13Z")
		require.NoError(t, err)

		require.Equal(t, withUTC, withOffset)
	})

	t.Run("a string is parsed into a UTC time", func(t *testing.T) {
		t.Parallel()

		v, err := common.NormalizeDateFilterValue("2026-05-21T15:09:13+04:00")
		require.NoError(t, err)

		ts, ok := v.(libtime.Time)
		require.True(t, ok, "expected a time value, got %T", v)
		require.True(t, ts.Time.Equal(stdtime.Date(2026, 5, 21, 11, 9, 13, 0, stdtime.UTC)))
	})

	t.Run("non-string values pass through unchanged", func(t *testing.T) {
		t.Parallel()

		now := libtime.Now()
		v, err := common.NormalizeDateFilterValue(now)
		require.NoError(t, err)
		require.Equal(t, now, v)
	})

	t.Run("an invalid date returns an error", func(t *testing.T) {
		t.Parallel()

		_, err := common.NormalizeDateFilterValue("not-a-date")
		require.Error(t, err)
	})
}
