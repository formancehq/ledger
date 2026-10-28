package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// pagedPreparedQueryServer serves totalItems transactions with the
// ExecutePreparedQuery pagination contract: a zero page size falls back to the
// server default and every page reports HasMore and the Next cursor.
type pagedPreparedQueryServer struct {
	servicepb.UnimplementedBucketServiceServer

	totalItems      int
	defaultPageSize uint32

	mu       sync.Mutex
	requests []*servicepb.ExecutePreparedQueryRequest
}

func (s *pagedPreparedQueryServer) ExecutePreparedQuery(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()

	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = s.defaultPageSize
	}

	start := 0
	if req.GetCursor() != "" {
		var err error

		start, err = strconv.Atoi(req.GetCursor())
		if err != nil {
			return nil, err
		}
	}

	end := min(start+int(pageSize), s.totalItems)

	cursor := &commonpb.PreparedQueryCursor{
		PageSize: pageSize,
		HasMore:  end < s.totalItems,
	}
	if cursor.GetHasMore() {
		cursor.Next = strconv.Itoa(end)
	}

	for id := start; id < end; id++ {
		cursor.TransactionData = append(cursor.TransactionData, &commonpb.Transaction{Id: uint64(id)})
	}

	return &servicepb.ExecutePreparedQueryResponse{
		Result: &servicepb.ExecutePreparedQueryResponse_Cursor{Cursor: cursor},
	}, nil
}

func (s *pagedPreparedQueryServer) observedCursors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	cursors := make([]string, 0, len(s.requests))
	for _, req := range s.requests {
		cursors = append(cursors, req.GetCursor())
	}

	return cursors
}

// Regression for Heal defect s2-cli-all-stops-at-first-page-1: --all printed
// only the first server page (100 results) and exited 0.
//
// Sequential because the registered command changes pterm writers and the
// stdout capture and isolated profile configuration modify process globals.
func TestQueriesExecuteAllFollowsCursorUntilLastPage(t *testing.T) {
	const totalItems = 120

	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")

	for _, tc := range []struct {
		name            string
		extraArgs       []string
		expectedCursors []string
	}{
		{
			name:            "explicit page size",
			extraArgs:       []string{"--page-size", "50"},
			expectedCursors: []string{"", "50", "100"},
		},
		{
			name:            "default page size",
			expectedCursors: []string{"", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "110"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &pagedPreparedQueryServer{totalItems: totalItems, defaultPageSize: 100}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, fixture)
			serveResult := make(chan error, 1)
			go func() { serveResult <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveResult)
			})

			root := newRootCommand()
			root.SetArgs(append([]string{
				"queries", "execute", "batch-txns", "--ledger", "paged", "--all", "--json",
				"--server", listener.Addr().String(), "--insecure",
				"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
				"--signing-key=", "--response-verify-key=",
				"--auth-token=test-token",
			}, tc.extraArgs...))

			stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
			require.NoError(t, err)
			originalStdout := os.Stdout
			os.Stdout = stdout
			t.Cleanup(func() {
				os.Stdout = originalStdout
				pterm.SetDefaultOutput(originalStdout)
				require.NoError(t, stdout.Close())
			})
			require.NoError(t, root.ExecuteContext(t.Context()))
			_, err = stdout.Seek(0, io.SeekStart)
			require.NoError(t, err)
			output, err := io.ReadAll(stdout)
			require.NoError(t, err)

			var transactions []struct {
				ID uint64 `json:"id"`
			}
			require.NoError(t, json.Unmarshal(output, &transactions), "output: %s", output)

			ids := make([]uint64, 0, len(transactions))
			for _, tx := range transactions {
				ids = append(ids, tx.ID)
			}

			expectedIDs := make([]uint64, 0, totalItems)
			for id := range uint64(totalItems) {
				expectedIDs = append(expectedIDs, id)
			}

			require.Equal(t, expectedIDs, ids, "--all must print every page, in order, exactly once")
			require.Equal(t, tc.expectedCursors, fixture.observedCursors(), "--all must follow Cursor.Next until HasMore is false")
		})
	}
}
