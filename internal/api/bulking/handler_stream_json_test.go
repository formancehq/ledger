package bulking

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledger "github.com/formancehq/ledger/internal"
)

func TestBulkStreamHandlerJSON(t *testing.T) {

	t.Parallel()

	type testCase struct {
		name               string
		stream             string
		expectedError      bool
		expectedStatusCode int

		expectScriptCount int
	}

	for _, testCase := range []testCase{
		{
			name: "nominal",
			stream: `
{"action": "CREATE_TRANSACTION", "data": {"postings": [{"source": "world", "amount": 100, "asset": "USD", "destination": "bank"}]}}
{"action": "CREATE_TRANSACTION", "data": {"postings": [{"source": "world", "amount": 200, "asset": "USD", "destination": "bank"}]}}
`,
			expectScriptCount: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			reader, writer := io.Pipe()
			defer func() { require.NoError(t, reader.Close()) }()
			defer func() { require.NoError(t, writer.Close()) }()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", reader)

			h := NewJSONStreamBulkHandler()
			send, receive, ok := h.GetChannels(w, r)

			if testCase.expectedError {
				require.False(t, ok)
				require.Equal(t, testCase.expectedStatusCode, w.Result().StatusCode)
				return
			}

			require.True(t, ok)

			writeDone := make(chan struct{})
			var writeErr, closeErr error
			go func() {
				defer close(writeDone)
				_, writeErr = writer.Write([]byte(testCase.stream))
				closeErr = writer.CloseWithError(writeErr)
			}()

			receiveClosed := false
			t.Cleanup(func() {
				// Unblock the producer and decoder even if an assertion fails.
				require.NoError(t, reader.Close())
				require.NoError(t, writer.Close())
				for range send {
				}
				if !receiveClosed {
					close(receive)
				}
				<-h.terminated
				<-writeDone
			})

			for id := range testCase.expectScriptCount {
				select {
				case _, ok := <-send:
					require.True(t, ok, "send channel closed before expected element")
				case <-time.After(100 * time.Millisecond):
					t.Fatal("should have received send channel")
				}
				select {
				case receive <- BulkElementResult{
					Data:      ledger.CreatedTransaction{},
					LogID:     uint64(id) + 1,
					ElementID: id,
				}:
				case <-time.After(100 * time.Millisecond):
					t.Fatal("should have been able to send on receive channel")
				}
			}

			select {
			case <-writeDone:
				require.NoError(t, writeErr)
				require.NoError(t, closeErr)
			case <-time.After(100 * time.Millisecond):
				t.Fatal("pipe writer should have completed")
			}
			select {
			case _, ok := <-send:
				require.False(t, ok, "send channel should have been closed")
			case <-time.After(100 * time.Millisecond):
				t.Fatal("send channel should have been closed")
			}
			close(receive)
			receiveClosed = true

			h.Terminate(w, r)

			require.Equal(t, http.StatusOK, w.Result().StatusCode)

			response, ok := api.DecodeSingleResponse[[]APIResult](t, w.Result().Body)
			require.True(t, ok)
			require.Len(t, response, testCase.expectScriptCount)
		})
	}
}
