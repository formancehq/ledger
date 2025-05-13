//
//
//

package system

import (
	context "context"
	reflect "reflect"

	bunpaginate "github.com/formancehq/go-libs/v3/bun/bunpaginate"
	metadata "github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
	ledger0 "github.com/formancehq/ledger/internal/controller/ledger"
	common "github.com/formancehq/ledger/internal/storage/common"
	gomock "go.uber.org/mock/gomock"
)

type MockStore struct {
	ctrl     *gomock.Controller
	recorder *MockStoreMockRecorder
}

type MockStoreMockRecorder struct {
	mock *MockStore
}

func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &MockStoreMockRecorder{mock}
	return mock
}

func (m *MockStore) EXPECT() *MockStoreMockRecorder {
	return m.recorder
}

func (m *MockStore) CreateLedger(arg0 context.Context, arg1 *ledger.Ledger) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateLedger", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStoreMockRecorder) CreateLedger(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateLedger", reflect.TypeOf((*MockStore)(nil).CreateLedger), arg0, arg1)
}

func (m *MockStore) DeleteLedgerMetadata(ctx context.Context, param, key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteLedgerMetadata", ctx, param, key)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStoreMockRecorder) DeleteLedgerMetadata(ctx, param, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteLedgerMetadata", reflect.TypeOf((*MockStore)(nil).DeleteLedgerMetadata), ctx, param, key)
}

func (m *MockStore) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLedger", ctx, name)
	ret0, _ := ret[0].(*ledger.Ledger)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStoreMockRecorder) GetLedger(ctx, name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLedger", reflect.TypeOf((*MockStore)(nil).GetLedger), ctx, name)
}

// ListBucketsWithStatus mocks base method.
func (m *MockStore) ListBucketsWithStatus(ctx context.Context) ([]BucketWithStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListBucketsWithStatus", ctx)
	ret0, _ := ret[0].([]BucketWithStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListBucketsWithStatus indicates an expected call of ListBucketsWithStatus.
func (mr *MockStoreMockRecorder) ListBucketsWithStatus(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListBucketsWithStatus", reflect.TypeOf((*MockStore)(nil).ListBucketsWithStatus), ctx)
}

func (m *MockStore) ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListLedgers", ctx, query)
	ret0, _ := ret[0].(*bunpaginate.Cursor[ledger.Ledger])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStoreMockRecorder) ListLedgers(ctx, query any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListLedgers", reflect.TypeOf((*MockStore)(nil).ListLedgers), ctx, query)
}

func (m *MockStore) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkBucketAsDeleted", ctx, bucketName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStoreMockRecorder) MarkBucketAsDeleted(ctx, bucketName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkBucketAsDeleted", reflect.TypeOf((*MockStore)(nil).MarkBucketAsDeleted), ctx, bucketName)
}

func (m *MockStore) OpenLedger(arg0 context.Context, arg1 string) (ledger0.Store, *ledger.Ledger, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenLedger", arg0, arg1)
	ret0, _ := ret[0].(ledger0.Store)
	ret1, _ := ret[1].(*ledger.Ledger)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (mr *MockStoreMockRecorder) OpenLedger(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenLedger", reflect.TypeOf((*MockStore)(nil).OpenLedger), arg0, arg1)
}

func (m *MockStore) RestoreBucket(ctx context.Context, bucketName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RestoreBucket", ctx, bucketName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStoreMockRecorder) RestoreBucket(ctx, bucketName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RestoreBucket", reflect.TypeOf((*MockStore)(nil).RestoreBucket), ctx, bucketName)
}

func (m_2 *MockStore) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "UpdateLedgerMetadata", ctx, name, m)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStoreMockRecorder) UpdateLedgerMetadata(ctx, name, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateLedgerMetadata", reflect.TypeOf((*MockStore)(nil).UpdateLedgerMetadata), ctx, name, m)
}
