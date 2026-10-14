package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledger "github.com/formancehq/ledger/internal"
)

func TestLogsImport(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		expectStatusCode  int
		expectedErrorCode string
		returnErr         error
	}

	testCases := []testCase{
		{
			name: "nominal",
		},
		{
			name:              "undefined error",
			returnErr:         errors.New("unexpected error"),
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusNoContent
			}

			log := ledger.NewLog(ledger.CreatedTransaction{
				Transaction:     ledger.NewTransaction(),
				AccountMetadata: ledger.AccountMetadata{},
			})

			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.EXPECT().
				Import(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, stream chan ledger.Log) error {
					if tc.returnErr != nil {
						return tc.returnErr
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case logFromStream := <-stream:
						require.Equal(t, log, logFromStream)
						select {
						case <-time.After(time.Second):
							require.Fail(t, "stream should have been closed")
						case <-stream:
						}
						return nil
					}
				})

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

			buf := bytes.NewBuffer(nil)
			require.NoError(t, json.NewEncoder(buf).Encode(log))

			req := httptest.NewRequest(http.MethodPost, "/xxx/logs/import", buf)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode > 300 {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestLogsImportClosesStreamOnMalformedInput(t *testing.T) {
	t.Parallel()

	streamClosed := make(chan struct{})
	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		Import(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, stream chan ledger.Log) error {
			for range stream {
			}
			close(streamClosed)
			return nil
		})

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")
	req := httptest.NewRequest(http.MethodPost, "/xxx/logs/import", bytes.NewBufferString("{"))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	select {
	case <-streamClosed:
	case <-time.After(time.Second):
		t.Fatal("import stream was not closed after malformed input")
	}
}

func TestLogsImportCancellationClosesBlockedStreamSend(t *testing.T) {
	t.Parallel()

	log := ledger.NewLog(ledger.CreatedTransaction{
		Transaction:     ledger.NewTransaction(),
		AccountMetadata: ledger.AccountMetadata{},
	})
	body, err := json.Marshal(log)
	require.NoError(t, err)

	importerStarted := make(chan struct{})
	releaseImporter := make(chan struct{})
	importerObservedClose := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseImporter) }) }
	defer release()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		Import(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, stream chan ledger.Log) error {
			close(importerStarted)
			<-releaseImporter
			for range stream {
			}
			close(importerObservedClose)
			return nil
		})

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/xxx/logs/import", bytes.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		router.ServeHTTP(rec, req)
		close(handlerDone)
	}()

	select {
	case <-importerStarted:
	case <-time.After(time.Second):
		t.Fatal("importer did not start")
	}
	// Keep the importer from receiving, so the handler's unbuffered stream send
	// cannot succeed. Cancellation must release that send and close the stream.
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler remained blocked sending to the importer after cancellation")
	}
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	release()
	select {
	case <-importerObservedClose:
	case <-time.After(time.Second):
		t.Fatal("importer did not observe the closed stream after cancellation")
	}
}
