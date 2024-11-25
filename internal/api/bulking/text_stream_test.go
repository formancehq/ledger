package bulking

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseStream(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		stream        string
		expectedError bool
		expectedCount int
	}

	for _, testCase := range []testCase{
		{
			name:          "nominal",
			expectedCount: 1,
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
}
//end`,
		},
		{
			name:          "multiple scripts",
			expectedCount: 2,
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
}
//end
//script
send [USD 100] (
	source = @world
	destination = @bob
}
//end`,
		},
		{
			name: "no tags",
			stream: `
send [USD 100] (
	source = @world
	destination = @alice
}`,
			expectedError: true,
		},
		{
			name:          "no ending tag",
			expectedCount: 1,
			stream: `
//script
send [USD 100] (
	source = @world
	destination = @alice
}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if testCase.expectedError {
				_, err := ParseTextStream(bufio.NewScanner(bytes.NewBufferString(testCase.stream)))
				require.Error(t, err)
				return
			} else {
				scanner := bufio.NewScanner(bytes.NewBufferString(testCase.stream))
				for range testCase.expectedCount {
					ret, err := ParseTextStream(scanner)
					require.NoError(t, err)
					require.NotNil(t, ret)
				}
			}
		})
	}
}
