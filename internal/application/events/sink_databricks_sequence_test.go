//go:build databricks

package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// This connector exercises database/sql's NamedValueChecker before ExecContext,
// as the pinned Databricks driver does. These external driver interfaces are
// intentionally implemented directly rather than through mockgen.
type sequenceConnector struct {
	driver.Connector

	conn *sequenceConn
}

func (c sequenceConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }

type sequenceConn struct {
	driver.Conn

	query string
	args  []driver.NamedValue
	call  int
	err   error
}

func (*sequenceConn) Close() error { return nil }

func (*sequenceConn) CheckNamedValue(value *driver.NamedValue) error {
	converted, err := driver.DefaultParameterConverter.ConvertValue(value.Value)
	if err != nil {
		return err
	}
	value.Value = converted

	return nil
}

func (c *sequenceConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.call++
	c.query = query
	c.args = append([]driver.NamedValue(nil), args...)
	if c.err != nil {
		return nil, c.err
	}

	return driver.RowsAffected(1), nil
}

func newSequenceSink(t *testing.T, conn *sequenceConn) *DatabricksSink {
	t.Helper()
	db := sql.OpenDB(sequenceConnector{conn: conn})
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	return &DatabricksSink{db: db, qualifiedTable: "main.default.events"}
}

func TestDatabricksSequenceRangeThroughDatabaseSQL(t *testing.T) {
	t.Parallel()
	// Reproduce the original failure at the actual database/sql conversion
	// boundary; the driver's ExecContext is never reached for this raw value.
	rawConn := &sequenceConn{}
	rawSink := newSequenceSink(t, rawConn)
	_, rawErr := rawSink.db.ExecContext(t.Context(), "SELECT ?", uint64(math.MaxInt64)+1)
	require.ErrorContains(t, rawErr, "uint64")
	require.Zero(t, rawConn.call)

	for _, sequence := range []uint64{math.MaxInt64, uint64(math.MaxInt64) + 1, math.MaxUint64 - 1} {
		t.Run(strconv.FormatUint(sequence, 10), func(t *testing.T) {
			t.Parallel()
			conn := &sequenceConn{}
			sink := newSequenceSink(t, conn)
			require.NoError(t, sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: sequence}}))
			require.Equal(t, 1, conn.call)
			require.Contains(t, conn.query, "CAST(? AS DECIMAL(20,0))")
			require.Equal(t, strconv.FormatUint(sequence, 10), conn.args[0].Value)
		})
	}
	require.Contains(t, DatabricksCreateTableDDL("main.default.events"), "log_sequence DECIMAL(20,0)")
}

func TestDatabricksPublicationFailureDoesNotAdvanceCursor(t *testing.T) {
	t.Parallel()
	cause := errors.New("warehouse unavailable")
	conn := &sequenceConn{err: cause}
	sink := newSequenceSink(t, conn)
	proposer := NewMockProposer(gomock.NewController(t))
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
			var decoded raftcmdpb.Proposal
			require.NoError(t, decoded.UnmarshalVT(proposal.Data()))
			updates := decoded.GetTechnicalUpdates()
			require.Len(t, updates, 1)
			sinkUpdate := updates[0].GetEventsSink()
			require.NotNil(t, sinkUpdate.GetError())
			require.Zero(t, sinkUpdate.GetCursor())
			proposal.Resolve(nil, nil)

			return resolvedFuture(), nil
		},
	).Times(1)
	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test", proposer, builder, logging.Testing(), DefaultEmitterConfig())
	sequence := uint64(math.MaxInt64) + 1
	err := emitter.publishBatch(t.Context(), []*eventspb.Event{{LogSequence: sequence}})
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, conn.call)
	require.Equal(t, strconv.FormatUint(sequence, 10), conn.args[0].Value)
	require.Contains(t, conn.query, "CAST(? AS DECIMAL(20,0))")
}
