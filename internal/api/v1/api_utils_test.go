package v1

import (
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"

	ledger "github.com/formancehq/ledger/internal"

	"go.uber.org/mock/gomock"
)

func newTestingSystemController(t *testing.T, expectedSchemaCheck bool) (*systemcontroller.MockController, *ledgercontroller.MockController) {
	ctrl := gomock.NewController(t)
	mockLedger := ledgercontroller.NewMockController(ctrl)
	backend := systemcontroller.NewMockController(ctrl)
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
