// Code generated by MockGen. DO NOT EDIT.
//
// Generated by this command:
//
//	mockgen -write_source_comment=false -write_package_comment=false -source bucket.go -destination bucket_generated_test.go -package bucket . Bucket
package bucket

import (
	context "context"
	reflect "reflect"

	migrations "github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	bun "github.com/uptrace/bun"
	gomock "go.uber.org/mock/gomock"
)

// MockBucket is a mock of Bucket interface.
type MockBucket struct {
	ctrl     *gomock.Controller
	recorder *MockBucketMockRecorder
}

// MockBucketMockRecorder is the mock recorder for MockBucket.
type MockBucketMockRecorder struct {
	mock *MockBucket
}

// NewMockBucket creates a new mock instance.
func NewMockBucket(ctrl *gomock.Controller) *MockBucket {
	mock := &MockBucket{ctrl: ctrl}
	mock.recorder = &MockBucketMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBucket) EXPECT() *MockBucketMockRecorder {
	return m.recorder
}

// AddLedger mocks base method.
func (m *MockBucket) AddLedger(ctx context.Context, db bun.IDB, ledger ledger.Ledger) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddLedger", ctx, db, ledger)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddLedger indicates an expected call of AddLedger.
func (mr *MockBucketMockRecorder) AddLedger(ctx, db, ledger any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddLedger", reflect.TypeOf((*MockBucket)(nil).AddLedger), ctx, db, ledger)
}

// GetMigrationsInfo mocks base method.
func (m *MockBucket) GetMigrationsInfo(ctx context.Context, db bun.IDB) ([]migrations.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrationsInfo", ctx, db)
	ret0, _ := ret[0].([]migrations.Info)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMigrationsInfo indicates an expected call of GetMigrationsInfo.
func (mr *MockBucketMockRecorder) GetMigrationsInfo(ctx, db any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrationsInfo", reflect.TypeOf((*MockBucket)(nil).GetMigrationsInfo), ctx, db)
}

// HasMinimalVersion mocks base method.
func (m *MockBucket) HasMinimalVersion(ctx context.Context, db bun.IDB) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasMinimalVersion", ctx, db)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasMinimalVersion indicates an expected call of HasMinimalVersion.
func (mr *MockBucketMockRecorder) HasMinimalVersion(ctx, db any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasMinimalVersion", reflect.TypeOf((*MockBucket)(nil).HasMinimalVersion), ctx, db)
}

// IsInitialized mocks base method.
func (m *MockBucket) IsInitialized(arg0 context.Context, arg1 bun.IDB) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsInitialized", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsInitialized indicates an expected call of IsInitialized.
func (mr *MockBucketMockRecorder) IsInitialized(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsInitialized", reflect.TypeOf((*MockBucket)(nil).IsInitialized), arg0, arg1)
}

// IsUpToDate mocks base method.
func (m *MockBucket) IsUpToDate(ctx context.Context, db bun.IDB) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsUpToDate", ctx, db)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsUpToDate indicates an expected call of IsUpToDate.
func (mr *MockBucketMockRecorder) IsUpToDate(ctx, db any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsUpToDate", reflect.TypeOf((*MockBucket)(nil).IsUpToDate), ctx, db)
}

// Migrate mocks base method.
func (m *MockBucket) Migrate(ctx context.Context, db bun.IDB, opts ...migrations.Option) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx, db}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Migrate", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Migrate indicates an expected call of Migrate.
func (mr *MockBucketMockRecorder) Migrate(ctx, db any, opts ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, db}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Migrate", reflect.TypeOf((*MockBucket)(nil).Migrate), varargs...)
}

// MockFactory is a mock of Factory interface.
type MockFactory struct {
	ctrl     *gomock.Controller
	recorder *MockFactoryMockRecorder
}

// MockFactoryMockRecorder is the mock recorder for MockFactory.
type MockFactoryMockRecorder struct {
	mock *MockFactory
}

// NewMockFactory creates a new mock instance.
func NewMockFactory(ctrl *gomock.Controller) *MockFactory {
	mock := &MockFactory{ctrl: ctrl}
	mock.recorder = &MockFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFactory) EXPECT() *MockFactoryMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockFactory) Create(name string) Bucket {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", name)
	ret0, _ := ret[0].(Bucket)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockFactoryMockRecorder) Create(name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockFactory)(nil).Create), name)
}

// GetMigrator mocks base method.
func (m *MockFactory) GetMigrator(b string, db bun.IDB) *migrations.Migrator {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrator", b, db)
	ret0, _ := ret[0].(*migrations.Migrator)
	return ret0
}

// GetMigrator indicates an expected call of GetMigrator.
func (mr *MockFactoryMockRecorder) GetMigrator(b, db any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrator", reflect.TypeOf((*MockFactory)(nil).GetMigrator), b, db)
}
