package bulking

import (
	"github.com/formancehq/go-libs/v2/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBulkHandlerText(t *testing.T) {

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
//script
send [USD 100] (
	source = @world
	destination = @alice
)
//end
//script
send [USD 100] (
	source = @world
	destination = @bob
)
//end
`,
			expectScriptCount: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			reader, writer := io.Pipe()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", reader)

			h := NewScriptStreamBulkHandler()
			send, receive, ok := h.GetChannels(w, r)

			if testCase.expectedError {
				require.False(t, ok)
				require.Equal(t, testCase.expectedStatusCode, w.Result().StatusCode)
				return
			}

			require.True(t, ok)

			_, err := writer.Write([]byte(testCase.stream))
			require.NoError(t, err)

			for id := range testCase.expectScriptCount {
				select {
				case <-send:
				case <-time.After(100 * time.Millisecond):
					t.Fatal("should have received send channel")
				}
				select {
				case receive <- BulkElementResult{
					Data:      ledger.CreatedTransaction{},
					LogID:     id + 1,
					ElementID: id,
				}:
				case <-time.After(100 * time.Millisecond):
					t.Fatal("should have been able to send on receive channel")
				}
			}

			require.NoError(t, writer.Close())
			close(receive)

			h.Terminate(w, r)

			require.Equal(t, http.StatusOK, w.Result().StatusCode)

			response, ok := api.DecodeSingleResponse[[]APIResult](t, w.Result().Body)
			require.True(t, ok)
			require.Len(t, response, testCase.expectScriptCount)
		})
	}
}
