package mirror

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestManager_PostgresParseErrorDoesNotDiscloseCredentials(t *testing.T) {
	t.Parallel()
	const secret = "MIRROR_MANAGER_SECRET_b81e:/+@"
	for name, password := range map[string]string{"raw": secret, "encoded": url.QueryEscape(secret)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			builder, store := newTestBuilder(t)
			dsn := "postgres://user@localhost/ledger?password=" + password + "&connect_timeout=invalid"
			info := mirrorLedgerInfo("diagnostic-ledger", "")
			info.MirrorSource.Type = &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: dsn},
			}
			saveLedgerInfo(t, store, info)
			var logs bytes.Buffer
			log := logrus.New()
			log.SetOutput(&logs)
			log.SetFormatter(&logrus.JSONFormatter{})
			m := NewManager(store, errProposer{}, builder, logging.NewLogrus(log), signal.NewNotifications(), noop.NewMeterProvider(), 0)
			// Drive real reconciliation synchronously: no worker should start and no
			// database access should be needed to diagnose an invalid timeout.
			m.OnLeadershipChange(true)
			defer m.cancelInitialization()
			m.mu.Lock()
			m.reconcile()
			m.mu.Unlock()
			require.Empty(t, workerNames(m))
			require.Contains(t, logs.String(), "Failed to create mirror source")
			require.Contains(t, logs.String(), `"ledger":"diagnostic-ledger"`)
			require.Contains(t, logs.String(), "invalid connect_timeout")
			require.NotContains(t, logs.String(), secret)
			require.NotContains(t, logs.String(), url.QueryEscape(secret))
			handle, err := store.NewDirectReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, handle.Close()) }()
			stored, err := query.GetLedgerByName(context.Background(), handle, info.GetName())
			require.NoError(t, err)
			require.Equal(t, dsn, stored.GetMirrorSource().GetPostgres().GetDsn())
		})
	}
}
