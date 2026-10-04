package bulking

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
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
						LogID:     uint64(id) + 1,
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

func TestJSONBulkInvalidFractionalLegacyAmountReturnsValidationWithoutCallingController(t *testing.T) {
	t.Parallel()

	requestBody := `[{"action":"CREATE_TRANSACTION","data":{"script":{"vars":{"amount":{"asset":"USD","amount":1.25}}}}}]`
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(requestBody))
	handler := NewJSONBulkHandler(0)
	bulk, results, ok := handler.GetChannels(httptest.NewRecorder(), request)
	require.True(t, ok)

	controller := NewLedgerController(gomock.NewController(t))
	controller.EXPECT().CreateTransaction(gomock.Any(), gomock.Any()).Times(0)
	bulker := NewBulker(controller)
	require.NoError(t, bulker.Run(context.Background(), bulk, results, BulkingOptions{}))

	responseRecorder := httptest.NewRecorder()
	handler.Terminate(responseRecorder, request)
	require.Equal(t, http.StatusBadRequest, responseRecorder.Code)

	var response struct {
		Data []APIResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &response))
	require.Len(t, response.Data, 1)
	require.Equal(t, common.ErrValidation, response.Data[0].ErrorCode)
	require.Contains(t, response.Data[0].ErrorDescription, "amount must be an integer")
}
