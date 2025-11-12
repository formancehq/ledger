package drivers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/logging"
)

func TestRegisterDriver(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		fn          any
		expectError string
	}

	for _, testCase := range []testCase{
		{
			name: "nominal",
			fn: func(_ struct{}, _ logging.Logger) (*MockDriver, error) {
				return &MockDriver{}, nil
			},
		},
		{
			name: "invalid third arg",
			fn: func(_ struct{}, _ struct{}) (*MockDriver, error) {
				return &MockDriver{}, nil
			},
			expectError: "constructor arg 2 must be of kind logging.Logger",
		},
		{
			name: "invalid first return",
			fn: func(_ struct{}, _ logging.Logger) (struct{}, error) {
				return struct{}{}, nil
			},
			expectError: "return 0 must be of kind drivers.Driver",
		},
		{
			name: "invalid second return",
			fn: func(_ struct{}, _ logging.Logger) (*MockDriver, string) {
				return &MockDriver{}, ""
			},
			expectError: "return 1 must be of kind error",
		},
		{
			name: "invalid number of parameters",
			fn: func() (*MockDriver, string) {
				return &MockDriver{}, ""
			},
			expectError: "constructor must take two parameters",
		},
		{
			name: "invalid number of returned values",
			fn: func(_ struct{}, _ logging.Logger) *MockDriver {
				return &MockDriver{}
			},
			expectError: "constructor must return two values",
		},
		{
			name:        "invalid constructor type",
			fn:          "foo",
			expectError: "constructor must be a func",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockStore(ctrl)

			exporterRegistry := NewRegistry(logging.Testing(), mockStore)
			err := exporterRegistry.registerDriver("testing", testCase.fn)
			if testCase.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Equal(t, testCase.expectError, err.Error())
			}
		})
	}
}
