package v1_test

import (
	"testing"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/golang/mock/gomock"
)

func newTestingBackend(t *testing.T) (*backend.MockBackend, *backend.MockLedger) {
	ctrl := gomock.NewController(t)
	mockLedger := backend.NewMockLedger(ctrl)
	backend := backend.NewMockBackend(ctrl)
	backend.
		EXPECT().
		GetLedger(gomock.Any(), gomock.Any()).
		MinTimes(0).
		Return(mockLedger, nil)
	t.Cleanup(func() {
		ctrl.Finish()
	})
	return backend, mockLedger
}
