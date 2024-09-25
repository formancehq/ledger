package v1_test

import (
	"testing"

	"github.com/formancehq/ledger/v2/internal/storage/systemstore"

	"github.com/formancehq/ledger/v2/internal/api/backend"
	"go.uber.org/mock/gomock"
)

func newTestingBackend(t *testing.T, expectedSchemaCheck bool) (*backend.MockBackend, *backend.MockLedger) {
	ctrl := gomock.NewController(t)
	mockLedger := backend.NewMockLedger(ctrl)
	backend := backend.NewMockBackend(ctrl)
	backend.
		EXPECT().
		GetLedger(gomock.Any(), gomock.Any()).
		MinTimes(0).
		Return(&systemstore.Ledger{}, nil)
	t.Cleanup(func() {
		ctrl.Finish()
	})
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
