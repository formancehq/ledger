package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"gopkg.in/segmentio/analytics-go.v3"
)

type transport func(*http.Request) (*http.Response, error)

func (fn transport) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type Queue[ITEM any] struct {
	mu    sync.Mutex
	items []ITEM
}

func (s *Queue[ITEM]) Get() (ret ITEM, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return
	}
	ret = s.items[0]
	ok = true
	if len(s.items) == 1 {
		s.items = make([]ITEM, 0)
		return
	}
	s.items = s.items[1:]
	return
}

func (s *Queue[ITEM]) Put(i ITEM) *Queue[ITEM] {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, i)
	return s
}

func (s *Queue[ITEM]) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items) == 0
}

func NewQueue[ITEM any]() *Queue[ITEM] {
	return &Queue[ITEM]{}
}

type segmentBatch struct {
	Batch []analytics.Track `json:"batch"`
}

const (
	interval      = 10 * time.Millisecond
	version       = "100.0.0"
	applicationId = "foo"
	writeKey      = "key"
)

func EventuallyQueueNotEmpty[ITEM any](t *testing.T, queue *Queue[ITEM]) {
	require.Eventually(t, func() bool {
		return !queue.Empty()
	}, 10*interval, interval)
}

var emptyHttpResponse = &http.Response{
	Body:       io.NopCloser(bytes.NewReader([]byte{})),
	StatusCode: http.StatusOK,
}

func TestAnalytics(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		transport http.RoundTripper
	}
	queue := NewQueue[*http.Request]()
	firstCallChan := make(chan struct{})
	testCases := []testCase{
		{
			name: "nominal",
			transport: transport(func(request *http.Request) (*http.Response, error) {
				queue.Put(request)
				return emptyHttpResponse, nil
			}),
		},
		{
			name: "with error on backend",
			transport: transport(func(request *http.Request) (*http.Response, error) {
				select {
				case <-firstCallChan: // Enter this case only if the chan is closed
					queue.Put(request)
					return emptyHttpResponse, nil
				default:
					close(firstCallChan)
					return nil, errors.New("general error")
				}
			}),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockLedger := NewMockLedger(ctrl)
			backend := NewMockBackend(ctrl)
			backend.
				EXPECT().
				ListLedgers(gomock.Any()).
				AnyTimes().
				Return([]string{"default"}, nil)
			backend.
				EXPECT().
				AppID(gomock.Any()).
				AnyTimes().
				Return(applicationId, nil)
			backend.
				EXPECT().
				GetLedgerStore(gomock.Any(), "default").
				AnyTimes().
				Return(mockLedger, nil)
			t.Cleanup(func() {
				ctrl.Finish()
			})
			analyticsClient, err := analytics.NewWithConfig(writeKey, analytics.Config{
				BatchSize: 1,
				Transport: testCase.transport,
			})
			require.NoError(t, err)

			mockLedger.
				EXPECT().
				CountTransactions(gomock.Any()).
				AnyTimes().
				Return(10, nil)
			mockLedger.
				EXPECT().
				CountAccounts(gomock.Any()).
				AnyTimes().
				Return(20, nil)

			h := newHeartbeat(backend, analyticsClient, version, interval)
			go func() {
				require.NoError(t, h.Run(context.Background()))
			}()
			t.Cleanup(func() {
				require.NoError(t, h.Stop(context.Background()))
			})

			for i := 0; i < 10; i++ {
				EventuallyQueueNotEmpty(t, queue)
				request, ok := queue.Get()
				require.True(t, ok)

				username, password, ok := request.BasicAuth()
				require.True(t, ok)
				require.Equal(t, writeKey, username)
				require.Empty(t, password)

				batch := &segmentBatch{}
				require.NoError(t, json.NewDecoder(request.Body).Decode(batch))
				require.Len(t, batch.Batch, 1)

				track := batch.Batch[0]
				require.Equal(t, ApplicationStats, track.Event)
				require.Equal(t, version, track.Properties[VersionProperty])
				require.Equal(t, applicationId, track.AnonymousId)
			}
		})
	}
}
