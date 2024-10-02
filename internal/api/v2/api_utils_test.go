package v2

import (
	"testing"

	"go.uber.org/mock/gomock"
)

func newTestingSystemController(t *testing.T, expectedSchemaCheck bool) (*SystemController, *LedgerController) {
	ctrl := gomock.NewController(t)
	mockLedger := NewLedgerController(ctrl)
	backend := NewSystemController(ctrl)
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
