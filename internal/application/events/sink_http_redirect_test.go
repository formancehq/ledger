package events

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

type redirectRequest struct {
	Path, Method, Sequence string
	Body                   []byte
}

// This receiver records HTTP acknowledgments, not remote business processing.
type redirectReceiver struct {
	mu           sync.Mutex
	status       int
	failSequence string
	recovered    bool
	requests     []redirectRequest
	acknowledged []redirectRequest
}

func (r *redirectReceiver) serve(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record := redirectRequest{req.URL.Path, req.Method, req.Header.Get("X-Log-Sequence"), body}
	r.requests = append(r.requests, record)
	if req.URL.Path == "/webhook" && !r.recovered && r.status != 0 && (r.failSequence == "" || r.failSequence == record.Sequence) {
		http.Redirect(w, req, "/login", r.status)

		return
	}
	if req.Method == http.MethodPost && len(body) > 0 {
		r.acknowledged = append(r.acknowledged, record)
	}
	w.WriteHeader(http.StatusOK)
}

func (r *redirectReceiver) snapshot() ([]redirectRequest, []redirectRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]redirectRequest(nil), r.requests...), append([]redirectRequest(nil), r.acknowledged...)
}

func TestHTTPSinkRedirectEmitterCursorAndRetry(t *testing.T) {
	t.Parallel()
	for _, status := range []int{0, 301, 302, 303, 307, 308} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			receiver := &redirectReceiver{status: status}
			server := httptest.NewServer(http.HandlerFunc(receiver.serve))
			defer server.Close()
			sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL + "/webhook", Format: FormatJSON})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			builder, store := newTestBuilder(t)
			// Persist two real logs, then drive the same batch boundary used by run.
			session := store.OpenWriteSession()
			logs := make([]*commonpb.Log, 0, 2)
			for _, seq := range []uint64{1, 2} {
				logs = append(logs, &commonpb.Log{Sequence: seq, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: fmt.Sprintf("ledger-%d", seq), CreatedAt: &commonpb.Timestamp{Data: 1000}}}}})
			}
			require.NoError(t, state.AppendLogs(session, logs))
			require.NoError(t, state.SetAppliedIndex(session, 1))
			require.NoError(t, session.Commit())
			proposer := &capturingProposer{}
			emitter := NewEmitter(store, sink, "redirect", proposer, builder, logging.Testing(), DefaultEmitterConfig())
			cursor, _, publishErr := emitter.processLogBatch(context.Background(), 0)
			require.Len(t, proposer.captured, 1)
			proposal := &raftcmdpb.Proposal{}
			require.NoError(t, proposal.UnmarshalVT(proposer.captured[0]))
			update := proposal.GetTechnicalUpdates()[0].GetEventsSink()
			requests, acknowledged := receiver.snapshot()
			t.Logf("status=%d publishErr=%v returnedCursor=%d proposedCursor=%d HTTP-acknowledged POSTs=%d", status, publishErr, cursor, update.GetCursor(), len(acknowledged))
			for _, request := range requests {
				t.Logf("request path=%s method=%s sequence=%s bodyBytes=%d", request.Path, request.Method, request.Sequence, len(request.Body))
			}
			if status == 301 || status == 302 || status == 303 {
				// Do not abort: on the vulnerable source the subsequent restart proves
				// that the incorrectly proposed cursor actually excludes both logs.
				assert.Error(t, publishErr)
				assert.Zero(t, cursor)
				assert.Zero(t, update.GetCursor())
				assert.Empty(t, acknowledged)
				assert.Len(t, requests, 1, "method-changing redirect must not reach /login")
				if publishErr != nil {
					assert.Contains(t, publishErr.Error(), fmt.Sprintf("unexpected status code: %d", status))
				}
			} else {
				require.NoError(t, publishErr)
				require.Equal(t, uint64(2), cursor)
				require.Equal(t, uint64(2), update.GetCursor())
				require.Len(t, acknowledged, 2)
				for _, ack := range acknowledged {
					require.Equal(t, http.MethodPost, ack.Method)
					require.NotEmpty(t, ack.Body)
				}
				if status != 0 {
					require.Len(t, requests, 4)
					require.Equal(t, requests[0].Body, requests[1].Body)
					require.Equal(t, requests[2].Body, requests[3].Body)
				}

				return
			}
			receiver.mu.Lock()
			receiver.recovered = true
			receiver.mu.Unlock()
			// Apply the captured cursor update to the fixture store, as the FSM
			// does after accepting the proposal, then read it back on restart.
			cursorSession := store.OpenWriteSession()
			require.NoError(t, state.SetSinkCursor(cursorSession, "redirect", update.GetCursor()))
			require.NoError(t, cursorSession.Commit())
			persistedCursor, err := query.ReadSinkCursor(store, "redirect")
			require.NoError(t, err)
			require.Equal(t, update.GetCursor(), persistedCursor)
			// A fresh emitter resets backoff as on leader restart.
			restarted := NewEmitter(store, sink, "redirect", proposer, builder, logging.Testing(), DefaultEmitterConfig())
			resumedCursor, _, err := restarted.processLogBatch(context.Background(), persistedCursor)
			require.NoError(t, err)
			require.Equal(t, uint64(2), resumedCursor)
			resumedRequests, resumedAcknowledged := receiver.snapshot()
			t.Logf("restart cursor=%d additional requests=%d acknowledged POSTs=%d", update.GetCursor(), len(resumedRequests)-len(requests), len(resumedAcknowledged))
			require.Len(t, resumedAcknowledged, 2, "restart must retry the complete unacknowledged batch")
			require.Equal(t, "1", resumedAcknowledged[0].Sequence)
			require.Equal(t, "2", resumedAcknowledged[1].Sequence)
			require.Equal(t, requests[0].Body, resumedAcknowledged[0].Body)
			require.Len(t, resumedRequests, 3, "one failed POST plus exactly two retry POSTs")
		})
	}
}

func TestHTTPSinkRedirectPartialBatchRetry(t *testing.T) {
	t.Parallel()
	for _, status := range []int{301, 302, 303} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			receiver := &redirectReceiver{status: status, failSequence: "2"}
			server := httptest.NewServer(http.HandlerFunc(receiver.serve))
			defer server.Close()
			sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL + "/webhook", Format: FormatJSON})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			builder, store := newTestBuilder(t)
			proposer := &capturingProposer{}
			emitter := NewEmitter(store, sink, "partial", proposer, builder, logging.Testing(), DefaultEmitterConfig())
			batch := []*eventspb.Event{{LogSequence: 1}, {LogSequence: 2}}
			err = emitter.publishBatch(context.Background(), batch)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("posting event seq=2: unexpected status code: %d", status))
			requests, acknowledged := receiver.snapshot()
			require.Len(t, requests, 2)
			require.Len(t, acknowledged, 1)
			update := &raftcmdpb.Proposal{}
			require.NoError(t, update.UnmarshalVT(proposer.captured[0]))
			require.Zero(t, update.GetTechnicalUpdates()[0].GetEventsSink().GetCursor())
			receiver.mu.Lock()
			receiver.recovered = true
			receiver.mu.Unlock()
			require.NoError(t, emitter.publishBatch(context.Background(), batch))
			requests, acknowledged = receiver.snapshot()
			require.Len(t, requests, 4)
			require.Len(t, acknowledged, 3)
			require.Equal(t, []string{"1", "1", "2"}, []string{acknowledged[0].Sequence, acknowledged[1].Sequence, acknowledged[2].Sequence})
			require.Equal(t, requests[0].Body, requests[2].Body)
			require.Equal(t, requests[1].Body, requests[3].Body)
			require.Len(t, proposer.captured, 2)
			update.Reset()
			require.NoError(t, update.UnmarshalVT(proposer.captured[1]))
			require.Equal(t, uint64(2), update.GetTechnicalUpdates()[0].GetEventsSink().GetCursor())
			require.True(t, update.GetTechnicalUpdates()[0].GetEventsSink().GetClearError())
		})
	}
}
