package main

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type aggregateVolumesOutputServer struct {
	servicepb.UnimplementedBucketServiceServer

	asset string
}

func (s *aggregateVolumesOutputServer) AggregateVolumes(ctx context.Context, _ *servicepb.AggregateVolumesRequest) (*commonpb.AggregateResult, error) {
	profile, err := proto.Marshal(&servicepb.QueryProfile{BarrierDurationUs: 1})
	if err != nil {
		return nil, err
	}

	if err := grpc.SetTrailer(ctx, metadata.Pairs(cmdutil.MetadataKeyQueryProfileResult, string(profile))); err != nil {
		return nil, err
	}

	return &commonpb.AggregateResult{
		Volumes: []*commonpb.AggregatedVolume{{
			Asset:  s.asset,
			Input:  commonpb.NewUint256FromUint64(1234),
			Output: commonpb.NewUint256FromUint64(0),
		}},
	}, nil
}

// With --analyze and --rescale, an invalid asset must abort before anything is
// printed, including the query profile the fixture returns in its trailer. The
// valid-asset control proves the profile and table are captured, so their
// absence in the invalid case is meaningful.
//
// Sequential because the registered command changes pterm writers and the
// stdout capture and isolated profile configuration modify process globals.
func TestAccountsAggregateVolumesRescaleInvalidAssetPrintsNothing(t *testing.T) {
	const profileMarker = "Query Profile"

	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")

	for _, tc := range []struct {
		name    string
		asset   string
		wantErr bool
	}{
		{name: "valid asset renders profile and table", asset: "USD/2"},
		{name: "invalid asset prints nothing", asset: "USD/x", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, &aggregateVolumesOutputServer{asset: tc.asset})
			serveResult := make(chan error, 1)
			go func() { serveResult <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveResult)
			})

			root := newRootCommand()
			root.SilenceErrors = true
			root.SilenceUsage = true
			root.SetArgs([]string{
				"accounts", "aggregate-volumes", "--ledger", "main",
				"--server", listener.Addr().String(), "--insecure",
				"--tls-ca-cert=", "--tls-server-name=", "--result-file=",
				"--signing-key=", "--response-verify-key=",
				"--auth-token=test-token", "--analyze", "--rescale",
			})

			stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
			require.NoError(t, err)
			originalStdout := os.Stdout
			os.Stdout = stdout
			pterm.SetDefaultOutput(stdout)
			t.Cleanup(func() {
				os.Stdout = originalStdout
				pterm.SetDefaultOutput(originalStdout)
				require.NoError(t, stdout.Close())
			})

			err = root.ExecuteContext(t.Context())

			_, seekErr := stdout.Seek(0, io.SeekStart)
			require.NoError(t, seekErr)
			output, readErr := io.ReadAll(stdout)
			require.NoError(t, readErr)

			if !tc.wantErr {
				require.NoError(t, err)
				require.Contains(t, string(output), profileMarker)
				require.Contains(t, string(output), "12.34")

				return
			}

			require.ErrorContains(t, err, `invariant: asset "USD/x"`)
			require.NotContains(t, string(output), profileMarker)
			require.NotContains(t, string(output), "ASSET")
		})
	}
}
