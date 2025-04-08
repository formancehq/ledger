// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source ../../controller/system/controller.go -destination mocks_system_controller_test.go -package common --mock_names Controller=SystemController . Controller
//

package common

import (
	context "context"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	ledger0 "github.com/formancehq/ledger/internal/controller/ledger"
	common "github.com/formancehq/ledger/internal/storage/common"
	gomock "go.uber.org/mock/gomock"
)

// SystemController is a mock of Controller interface.
type SystemController struct {
	ctrl     *gomock.Controller
	recorder *SystemControllerMockRecorder
	isgomock struct{}
}

// SystemControllerMockRecorder is the mock recorder for SystemController.
type SystemControllerMockRecorder struct {
	mock *SystemController
}

// NewSystemController creates a new mock instance.
func NewSystemController(ctrl *gomock.Controller) *SystemController {
	mock := &SystemController{ctrl: ctrl}
	mock.recorder = &SystemControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *SystemController) EXPECT() *SystemControllerMockRecorder {
	return m.recorder
}

// CreateLedger mocks base method.
func (m *SystemController) CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateLedger", ctx, name, configuration)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateLedger indicates an expected call of CreateLedger.
func (mr *SystemControllerMockRecorder) CreateLedger(ctx, name, configuration any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateLedger", reflect.TypeOf((*SystemController)(nil).CreateLedger), ctx, name, configuration)
}

// DeleteLedgerMetadata mocks base method.
func (m *SystemController) DeleteLedgerMetadata(ctx context.Context, param, key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteLedgerMetadata", ctx, param, key)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteLedgerMetadata indicates an expected call of DeleteLedgerMetadata.
func (mr *SystemControllerMockRecorder) DeleteLedgerMetadata(ctx, param, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteLedgerMetadata", reflect.TypeOf((*SystemController)(nil).DeleteLedgerMetadata), ctx, param, key)
}

// GetLedger mocks base method.
func (m *SystemController) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLedger", ctx, name)
	ret0, _ := ret[0].(*ledger.Ledger)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLedger indicates an expected call of GetLedger.
func (mr *SystemControllerMockRecorder) GetLedger(ctx, name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLedger", reflect.TypeOf((*SystemController)(nil).GetLedger), ctx, name)
}

// GetLedgerController mocks base method.
func (m *SystemController) GetLedgerController(ctx context.Context, name string) (ledger0.Controller, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLedgerController", ctx, name)
	ret0, _ := ret[0].(ledger0.Controller)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLedgerController indicates an expected call of GetLedgerController.
func (mr *SystemControllerMockRecorder) GetLedgerController(ctx, name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLedgerController", reflect.TypeOf((*SystemController)(nil).GetLedgerController), ctx, name)
}

// ListLedgers mocks base method.
func (m *SystemController) ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLedgers", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Ledger])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListLedgers indicates an expected call of ListLedgers.
func (mr *SystemControllerMockRecorder) ListLedgers(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLedgers", reflect.TypeOf((*SystemController)(nil).ListLedgers), ctx, query)
}

// UpdateLedgerMetadata mocks base method.
func (m_2 *SystemController) UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "UpdateLedgerMetadata", ctx, name, m)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateLedgerMetadata indicates an expected call of UpdateLedgerMetadata.
func (mr *SystemControllerMockRecorder) UpdateLedgerMetadata(ctx, name, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateLedgerMetadata", reflect.TypeOf((*SystemController)(nil).UpdateLedgerMetadata), ctx, name, m)
}
