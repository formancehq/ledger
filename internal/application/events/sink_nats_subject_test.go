//go:build nats

package events

import (
	"net/url"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestNATSSinkSubjectLedgerToken(t *testing.T) {
	t.Parallel()
	sink := &NATSSink{topic: "events"}
	names := []string{"orders", "a..b", ".orders", "orders.", "a.b", "_system", ".", strings.Repeat(".", dal.LedgerNameFixedSize)}
	// Every admitted character is exercised at both token boundaries.
	for _, c := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" {
		names = append(names, string(c)+"name"+string(c))
	}
	seen := map[string]string{}
	for _, name := range names {
		require.Nil(t, domain.ValidateLedgerName(name))
		subject := sink.subject(&eventspb.Event{Ledger: name, Type: commonpb.EventType_CREATED_LEDGER})
		require.True(t, server.IsValidSubject(subject), subject)
		parts := strings.Split(subject, ".")
		require.Len(t, parts, 3)
		require.NotEqual(t, "_system", parts[1])
		decoded, err := url.PathUnescape(parts[1])
		require.NoError(t, err)
		require.Equal(t, name, decoded)
		previous, exists := seen[subject]
		require.False(t, exists, "collision between %q and %q", previous, name)
		seen[subject] = name
	}
	require.Equal(t, "events._system.created_ledger", sink.subject(&eventspb.Event{Type: commonpb.EventType_CREATED_LEDGER}))
	// Percent is not currently admitted, but must never alias an escaped dot.
	require.Equal(t, "events.%252E.created_ledger", sink.subject(&eventspb.Event{Ledger: "%2E", Type: commonpb.EventType_CREATED_LEDGER}))
}
