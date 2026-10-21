package bulking

import (
	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
			t.Cleanup(func() {
				_ = reader.Close()
				_ = writer.Close()
			})

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

			// Consume elements while writing: the decoder need not buffer the whole stream.
			writeResult := make(chan error, 1)
			go func() {
				_, err := writer.Write([]byte(testCase.stream))
				writeResult <- err
			}()

			for id := range testCase.expectScriptCount {
				select {
				case <-send:
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
			case err := <-writeResult:
				require.NoError(t, err)
			case <-time.After(100 * time.Millisecond):
				t.Fatal("stream write should have completed")
			}
			require.NoError(t, writer.Close())
			select {
			case <-send:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("send channel should have been closed")
			}
			close(receive)

			h.Terminate(w, r)

			require.Equal(t, http.StatusOK, w.Result().StatusCode)

			response, ok := api.DecodeSingleResponse[[]APIResult](t, w.Result().Body)
			require.True(t, ok)
			require.Len(t, response, testCase.expectScriptCount)
		})
	}
}
