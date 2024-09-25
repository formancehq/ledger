package v2_test

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v2/internal/api/backend"
)

func newTestingBackend(t *testing.T, expectedSchemaCheck bool) (*backend.MockBackend, *backend.MockLedger) {
	ctrl := gomock.NewController(t)
	mockLedger := backend.NewMockLedger(ctrl)
	backend := backend.NewMockBackend(ctrl)
	backend.
		EXPECT().
		GetLedgerEngine(gomock.Any(), gomock.Any()).
		MinTimes(0).
		Return(mockLedger, nil)
	t.Cleanup(func() {
		ctrl.Finish()
	})
	if expectedSchemaCheck {
		mockLedger.EXPECT().
			IsDatabaseUpToDate(gomock.Any()).
			Return(true, nil)
	}
	return backend, mockLedger
}
