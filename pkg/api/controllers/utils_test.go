package controllers_test

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func newTestingBackend(t *testing.T) (*MockBackend, *MockLedger) {
	ctrl := gomock.NewController(t)
	mockLedger := NewMockLedger(ctrl)
	backend := NewMockBackend(ctrl)
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
