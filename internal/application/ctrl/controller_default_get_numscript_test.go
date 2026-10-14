package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestDefaultController_GetNumscript_InvalidVersion asserts that reading an
// existing template with a selector that is not a (partial) semver is refused
// as an invalid version, like the save path, rather than reported as a
// missing template.
func TestDefaultController_GetNumscript_InvalidVersion(t *testing.T) {
	t.Parallel()

	const (
		ledger = "ledger1"
		name   = "script"
	)

	store := newCtrlTestStore(t)
	attrs := attributes.New()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{
		Name:      ledger,
		CreatedAt: commonpb.NewTimestamp(libtime.Now()),
	}))
	_, err := attrs.NumscriptVersion.Set(batch,
		domain.NumscriptVersionKey{LedgerName: ledger, Name: name}.Bytes(),
		&commonpb.NumscriptVersionValue{Version: "1.0.0"})
	require.NoError(t, err)
	_, err = attrs.NumscriptContent.Set(batch,
		domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: "1.0.0"}.Bytes(),
		&commonpb.NumscriptInfo{Name: name, Version: "1.0.0", Content: "send [USD/2 1] (source = @world destination = @a)"})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	logger := logging.FromContext(logging.TestingContext())
	ctrl := NewDefaultController(nil, store, logger, attrs, nil, nil, noop.NewMeterProvider().Meter("test"))

	info, err := ctrl.GetNumscript(context.Background(), ledger, name, "1.0.0")
	require.NoError(t, err)
	require.Equal(t, "1.0.0", info.GetVersion())

	for _, version := range []string{"abc", "1.x", "1.0.0.0", "1..0", "v1"} {
		_, err := ctrl.GetNumscript(context.Background(), ledger, name, version)

		var invalid *domain.ErrNumscriptInvalidVersion
		require.ErrorAs(t, err, &invalid, "version %q", version)
		require.Equal(t, version, invalid.Version)
	}
}
