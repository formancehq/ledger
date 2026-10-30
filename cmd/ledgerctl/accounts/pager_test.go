package accounts

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type accountPagerClient struct {
	servicepb.BucketServiceClient

	calls int
}

func (c *accountPagerClient) ListAccounts(context.Context, *servicepb.ListAccountsRequest, ...grpc.CallOption) (servicepb.BucketService_ListAccountsClient, error) {
	c.calls++

	return &accountPagerStream{}, nil
}

type accountPagerStream struct {
	grpc.ServerStreamingClient[commonpb.Account]

	read bool
}

func (s *accountPagerStream) Recv() (*commonpb.Account, error) {
	if s.read {
		return nil, io.EOF
	}
	s.read = true

	return &commonpb.Account{Address: "users:alice"}, nil
}

func (*accountPagerStream) Trailer() metadata.MD {
	return metadata.Pairs(cmdutil.NextCursorTrailerKey, "next-page")
}

func TestPagerStopsAfterFirstPageAtEOF(t *testing.T) {
	stdin, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	t.Cleanup(func() { _ = stdin.Close() })
	oldStdin := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() { os.Stdin = oldStdin })

	var output bytes.Buffer
	pterm.SetDefaultOutput(&output)
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout) })
	infoWriter := pterm.Info.Writer
	pterm.Info.Writer = &output
	t.Cleanup(func() { pterm.Info.Writer = infoWriter })

	client := &accountPagerClient{}
	cmd := NewListCommand()
	cmd.SetContext(context.Background())
	cmd.Flags().String("auth-token", "test", "")
	err = fetchAccountsWithPager(cmd, client, "ledger", cmdutil.PaginationFlags{}, nil, cmdutil.ConsistencyFlags{}, false, nil)
	if err != nil {
		t.Fatalf("pager should stop successfully: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("expected one page request, got %d", client.calls)
	}
	for _, want := range []string{"users:alice", "next-page"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("missing %q in output: %q", want, output.String())
		}
	}
}
