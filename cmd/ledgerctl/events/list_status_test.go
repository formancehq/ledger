package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAppendSinkStatusRows(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		cursor       uint64
		errorMessage string
		hasStatus    bool
		occurredAt   *commonpb.Timestamp
		wantLabel    string
		wantValue    string
	}{
		{name: "no status", wantLabel: "Status", wantValue: "pending"},
		{name: "no delivery", hasStatus: true, wantLabel: "Status", wantValue: "pending"},
		{name: "startup failure", hasStatus: true, errorMessage: "sink startup: connection refused", occurredAt: commonpb.NewTimestamp(libtime.New(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC))), wantLabel: "Error", wantValue: "sink startup: connection refused"},
		{name: "delivered", hasStatus: true, cursor: 12, wantLabel: "Status", wantValue: "healthy"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := appendSinkStatusRows(nil, test.cursor, test.errorMessage, test.occurredAt, test.hasStatus)
			require.Equal(t, "Cursor", rows[0][0])
			require.Equal(t, test.wantLabel, rows[1][0])
			require.Contains(t, rows[1][1], test.wantValue)
			if test.occurredAt != nil {
				require.Equal(t, []string{"Error At", "2026-10-08T12:00:00Z"}, rows[2])
			}
		})
	}
}
