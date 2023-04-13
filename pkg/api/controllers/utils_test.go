package controllers_test

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Encode(t *testing.T, v interface{}) []byte {
	data, err := json.Marshal(v)
	assert.NoError(t, err)
	return data
}

func Buffer(t *testing.T, v interface{}) *bytes.Buffer {
	return bytes.NewBuffer(Encode(t, v))
}

func Decode(t *testing.T, reader io.Reader, v interface{}) {
	err := json.NewDecoder(reader).Decode(v)
	require.NoError(t, err)
}

func DecodeSingleResponse[T any](t *testing.T, reader io.Reader) (T, bool) {
	res := sharedapi.BaseResponse[T]{}
	Decode(t, reader, &res)
	return *res.Data, true
}

func DecodeCursorResponse[T any](t *testing.T, reader io.Reader) *sharedapi.Cursor[T] {
	res := sharedapi.BaseResponse[T]{}
	Decode(t, reader, &res)
	return res.Cursor
}

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
