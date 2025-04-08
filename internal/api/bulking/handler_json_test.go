package bulking

import (
	"bytes"
	"encoding/json"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBulkHandlerJSON(t *testing.T) {

	t.Parallel()

	type testCase struct {
		name               string
		bulk               []BulkElement
		expectedError      bool
		expectedStatusCode int
	}
	const maxBulkSize = 3

	for _, testCase := range []testCase{
		{
			name: "nominal",
			bulk: []BulkElement{
				{
					Action: ActionCreateTransaction,
					Data: TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: ledgercontroller.Script{
								Plain: `
send [USD 100] (
	source = @world
	destination = @alice
)
`,
							},
						},
					},
				},
			},
		},
		{
			name:               "bulk exceeded max size",
			expectedError:      true,
			expectedStatusCode: http.StatusRequestEntityTooLarge,
			bulk: func() []BulkElement {
				ret := make([]BulkElement, 0)
				for range maxBulkSize + 1 {
					ret = append(ret, BulkElement{
						Action: ActionCreateTransaction,
						Data: TransactionRequest{
							Script: ledgercontroller.ScriptV1{
								Script: ledgercontroller.Script{
									Plain: `
send [USD 100] (
	source = @world
	destination = @alice
)
`,
								},
							},
						},
					})
				}

				return ret
			}(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			rawData, err := json.Marshal(testCase.bulk)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(rawData))

			h := NewJSONBulkHandler(maxBulkSize)
			send, receive, ok := h.GetChannels(w, r)

			if testCase.expectedError {
				require.False(t, ok)
				require.Equal(t, testCase.expectedStatusCode, w.Result().StatusCode)
				return
			}

			require.True(t, ok)

			for id, element := range testCase.bulk {
				select {
				case item := <-send:
					require.Equal(t, element, item)

					receive <- BulkElementResult{
						Data:      ledger.CreatedTransaction{},
						LogID:     id + 1,
						ElementID: id,
					}
				case <-time.After(100 * time.Millisecond):
					t.Fatal("should have receive an item on the send channel")
				}
			}

			select {
			case _, ok := <-send:
				require.False(t, ok)
			case <-time.After(100 * time.Millisecond):
				t.Fatal("send channel should have been closed since the bulk has been completely consumed")
			}

			close(receive)
			h.Terminate(w, r)

			require.Equal(t, http.StatusOK, w.Result().StatusCode)

			response, ok := api.DecodeSingleResponse[[]APIResult](t, w.Result().Body)
			require.True(t, ok)
			require.Len(t, response, len(testCase.bulk))
		})
	}
}
