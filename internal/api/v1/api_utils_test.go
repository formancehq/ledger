package v1

import (
	"testing"

	ledger "github.com/formancehq/ledger/internal"

	"go.uber.org/mock/gomock"
)

func newTestingSystemController(t *testing.T, expectedSchemaCheck bool) (*SystemController, *LedgerController) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockLedger := NewLedgerController(ctrl)
	backend := NewSystemController(ctrl)
	backend.
		EXPECT().
		GetLedger(gomock.Any(), gomock.Any()).
		MinTimes(0).
		Return(&ledger.Ledger{}, nil)
	t.Cleanup(func() {
		ctrl.Finish()
	})
	backend.
		EXPECT().
		GetLedgerController(gomock.Any(), gomock.Any()).
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
