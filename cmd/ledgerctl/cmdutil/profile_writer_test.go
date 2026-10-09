package cmdutil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/pterm/pterm"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRenderProfileToUsesScopedWriters(t *testing.T) {
	t.Parallel()

	writers := []io.Writer{
		pterm.DefaultTable.Writer,
		pterm.DefaultHeader.Writer,
		pterm.DefaultSection.Writer,
		pterm.DefaultBasicText.Writer,
		pterm.Warning.Writer,
	}
	var output bytes.Buffer
	cmdutil.RenderProfileTo(&output, &servicepb.QueryProfile{
		ServerDurationUs:  1,
		PrepareDurationUs: 2,
		RootIterator: &servicepb.IteratorProfile{
			Label: "parent iterator",
			Children: []*servicepb.IteratorProfile{{
				Label: "child iterator",
			}},
		},
	})
	cmdutil.RenderProfileTo(&output, nil)
	for _, want := range []string{
		"Query Profile", "Server Phase", "Execution Metric", "Iterator Tree",
		"parent iterator", "child iterator", "Server phase breakdown is inconsistent",
		"No profile data received from server.",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("explicit writer lost %q: %s", want, output.String())
		}
	}
	current := []io.Writer{
		pterm.DefaultTable.Writer,
		pterm.DefaultHeader.Writer,
		pterm.DefaultSection.Writer,
		pterm.DefaultBasicText.Writer,
		pterm.Warning.Writer,
	}
	for i, writer := range writers {
		if current[i] != writer {
			t.Fatalf("profile rendering changed global writer %d", i)
		}
	}
}

// The real native pager is used with both shared-result types. Failed fetches
// deliver their last received row and trailer, so the assertions protect the
// partial-result branch as well as successful --analyze + --json output.
func TestListAnalyzeJSONKeepsPayloadAndPartialResults(t *testing.T) {
	t.Parallel()

	profile, err := proto.Marshal(&servicepb.QueryProfile{ServerDurationUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"accounts", "transactions"} {
		for _, all := range []bool{false, true} {
			for _, partial := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/all=%t/partial=%t", resource, all, partial), func(t *testing.T) {
					t.Parallel()

					cmd := accounts.NewListCommand()
					if resource == "transactions" {
						cmd = transactions.NewListCommand()
					}
					cmd.SetContext(t.Context())
					var output, diagnostics bytes.Buffer
					cmd.SetOut(&output)
					cmd.SetErr(&diagnostics)
					for name, value := range map[string]string{
						"json": "true", "analyze": "true", "all": strconv.FormatBool(all),
						"cursor": "start", "page-size": "2", "reverse": "true",
					} {
						if err := cmd.Flags().Set(name, value); err != nil {
							t.Fatal(err)
						}
					}
					fetchErr := errors.New("stream closed after a received row")
					calls := 0
					pageResult := func(ctx context.Context, page cmdutil.PaginationFlags) (metadata.MD, error) {
						calls++
						if _, ok := ctx.Deadline(); !ok {
							t.Fatal("page fetch lost its timeout")
						}
						outgoing, _ := metadata.FromOutgoingContext(ctx)
						if got := outgoing.Get(cmdutil.MetadataKeyQueryProfile); len(got) != 1 || got[0] != "true" {
							t.Fatalf("page fetch lost profiling metadata: %v", outgoing)
						}
						if !page.Reverse {
							t.Fatal("page fetch lost reverse ordering")
						}
						if !all && page.PageSize != 2 {
							t.Fatalf("page size: %d", page.PageSize)
						}
						wantCursor := "start"
						if calls > 1 {
							wantCursor = "next"
						}
						if page.Cursor != wantCursor {
							t.Fatalf("cursor: got %q, want %q", page.Cursor, wantCursor)
						}
						trailer := metadata.Pairs(cmdutil.MetadataKeyQueryProfileResult, string(profile))
						if all && calls == 1 {
							trailer.Set(cmdutil.NextCursorTrailerKey, "next")

							return trailer, nil
						}
						trailer.Set(cmdutil.PreviousCursorTrailerKey, "previous")
						if partial || !all {
							trailer.Set(cmdutil.NextCursorTrailerKey, "resume")
						}
						if partial {
							return trailer, fetchErr
						}

						return trailer, nil
					}
					var runErr error
					if resource == "accounts" {
						runErr = accounts.RunListWithFetch(cmd, func(ctx context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
							trailer, err := pageResult(ctx, page)

							return []*commonpb.Account{{Address: "users:alice"}}, trailer, err
						})
					} else {
						runErr = transactions.RunListWithFetch(cmd, "precise", func(ctx context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error) {
							trailer, err := pageResult(ctx, page)

							return []*commonpb.Transaction{{Id: 9007199254740993}}, trailer, err
						})
					}
					if partial && !errors.Is(runErr, fetchErr) {
						t.Fatalf("lost fetch failure: %v", runErr)
					}
					if !partial && runErr != nil {
						t.Fatal(runErr)
					}
					wantRows := 1
					if all {
						wantRows = 2
					}
					if calls != wantRows {
						t.Fatalf("fetch count: %d, want %d without retries", calls, wantRows)
					}
					var rows []json.RawMessage
					if err := json.Unmarshal(output.Bytes(), &rows); err != nil {
						t.Fatalf("profile contaminated JSON %q: %v", output.String(), err)
					}
					if len(rows) != wantRows {
						t.Fatalf("lost received rows: %d, want %d", len(rows), wantRows)
					}
					if !strings.Contains(diagnostics.String(), "Query Profile") {
						t.Fatalf("profiling missing from stderr: %s", diagnostics.String())
					}
					if partial || !all {
						if !strings.Contains(diagnostics.String(), "next_cursor=resume") {
							t.Fatalf("partial page cursor missing: %s", diagnostics.String())
						}
					}
				})
			}
		}
	}
}

func TestListRescaleFailureDoesNotPrintProfile(t *testing.T) {
	t.Parallel()

	cmd := accounts.NewListCommand()
	cmd.Flags().Uint8("rescale", 0, "Display rescaling target")
	cmd.SetContext(t.Context())
	var output, diagnostics bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&diagnostics)
	for name, value := range map[string]string{"analyze": "true", "rescale": "0", "all": "true"} {
		if err := cmd.Flags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	profile, err := proto.Marshal(&servicepb.QueryProfile{ServerDurationUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	err = accounts.RunListWithFetch(cmd, func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
		return []*commonpb.Account{{
			Address: "users:alice",
			Volumes: []*commonpb.AccountVolume{{
				Asset: "USD/x",
				Volumes: &commonpb.VolumesWithBalance{
					Input:   commonpb.MustBigUintFromDecimal("1234"),
					Output:  commonpb.MustBigUintFromDecimal("0"),
					Balance: commonpb.MustSignedBigIntFromDecimal("1234"),
				},
			}},
		}}, metadata.Pairs(cmdutil.MetadataKeyQueryProfileResult, string(profile)), nil
	})
	if err == nil || !strings.Contains(err.Error(), `invariant: asset "USD/x"`) {
		t.Fatalf("expected the rescaling invariant, got %v", err)
	}
	if strings.Contains(output.String()+diagnostics.String(), "Query Profile") {
		t.Fatalf("profile printed before rendering validation: out=%s err=%s", output.String(), diagnostics.String())
	}
}

func TestListAnalyzeJSONEmptyPages(t *testing.T) {
	t.Parallel()

	profile, err := proto.Marshal(&servicepb.QueryProfile{ServerDurationUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"accounts", "transactions"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()

			cmd := accounts.NewListCommand()
			if resource == "transactions" {
				cmd = transactions.NewListCommand()
			}
			cmd.SetContext(t.Context())
			var output, diagnostics bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&diagnostics)
			for _, name := range []string{"json", "analyze"} {
				if err := cmd.Flags().Set(name, "true"); err != nil {
					t.Fatal(err)
				}
			}
			trailer := metadata.Pairs(cmdutil.MetadataKeyQueryProfileResult, string(profile))
			var runErr error
			if resource == "accounts" {
				runErr = accounts.RunListWithFetch(cmd, func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
					return nil, trailer, nil
				})
			} else {
				runErr = transactions.RunListWithFetch(cmd, "precise", func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error) {
					return nil, trailer, nil
				})
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			var rows []json.RawMessage
			if err := json.Unmarshal(output.Bytes(), &rows); err != nil {
				t.Fatalf("empty page is not valid JSON: %q: %v", output.String(), err)
			}
			if len(rows) != 0 || !strings.Contains(diagnostics.String(), "Query Profile") {
				t.Fatalf("empty page or profile changed: out=%s err=%s", output.String(), diagnostics.String())
			}
		})
	}
}

func TestListHumanAnalyzeKeepsProfileOnCommandOutput(t *testing.T) {
	t.Parallel()

	profile, err := proto.Marshal(&servicepb.QueryProfile{ServerDurationUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"accounts", "transactions"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()

			cmd := accounts.NewListCommand()
			if resource == "transactions" {
				cmd = transactions.NewListCommand()
			}
			cmd.SetContext(t.Context())
			var output, diagnostics bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&diagnostics)
			for _, name := range []string{"all", "analyze"} {
				if err := cmd.Flags().Set(name, "true"); err != nil {
					t.Fatal(err)
				}
			}
			trailer := metadata.Pairs(cmdutil.MetadataKeyQueryProfileResult, string(profile))
			var runErr error
			if resource == "accounts" {
				runErr = accounts.RunListWithFetch(cmd, func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
					return []*commonpb.Account{{Address: "users:alice"}}, trailer, nil
				})
			} else {
				runErr = transactions.RunListWithFetch(cmd, "precise", func(context.Context, cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error) {
					return []*commonpb.Transaction{{Id: 1}}, trailer, nil
				})
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			if !strings.Contains(output.String(), "Query Profile") || strings.Contains(diagnostics.String(), "Query Profile") {
				t.Fatalf("human profile moved away from stdout: out=%s err=%s", output.String(), diagnostics.String())
			}
		})
	}
}
