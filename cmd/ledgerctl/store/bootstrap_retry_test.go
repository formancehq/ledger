//go:build s3

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func newBootstrapRetryAdmission(t *testing.T, store *dal.Store) *admission.Admission {
	t.Helper()
	logger := logging.NopZap()
	meters := noop.NewMeterProvider()
	attrs := attributes.New()
	c, err := cache.New(1000, meters.Meter("bootstrap-retry"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attrs)
	keys := keystore.NewKeyStore()
	shared := state.NewSharedState()
	mocks := gomock.NewController(t)
	notifier := NewMockNotifier(mocks)
	notifier.EXPECT().NotifyLogsCommitted(gomock.Any()).AnyTimes()
	notifier.EXPECT().NotifyConfigChanged().AnyTimes()
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store,
		dal.NewSentinelFactory(store, false), meters, keys, shared, notifier, nil,
		"", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	require.NoError(t, state.NewRecovery(machine, store).RecoverState())
	next := machine.State.LastAppliedIndex + 1
	_, err = machine.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(next), Term: new(uint64(1)), Type: new(raftpb.EntryNormal)})
	require.NoError(t, err)
	tracker := node.NewIndexTracker(next + 1)
	builder := plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0)
	gate := health.NewMockWriteGate(mocks)
	gate.EXPECT().CheckWritesAllowed().Return(nil).AnyTimes()
	// Bypass transport only: the callback applies the bytes produced by real
	// admission, preserving plans, business logs and chained audit records.
	proposer := NewMockProposer(mocks)
	proposer.EXPECT().InitialIndex().Return(uint64(0)).AnyTimes()
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		result, applyErr := machine.ApplyEntries(ctx, store, &raftpb.Entry{
			Index: new(tracker.Next()), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: proposal.Data(),
		})
		require.NoError(t, applyErr)
		require.Len(t, result.Results, 1)
		require.NoError(t, result.Results[0].Error)
		tracker.Increment(1)
		proposal.Resolve(nil, nil)
		future := futures.New[state.ApplyResult]()
		future.Resolve(result.Results[0], nil)

		return future, nil
	}).AnyTimes()

	return admission.NewAdmission(store, logger, proposer,
		builder, meters, gate, keys, shared, attrs, numscript.NewNumscriptCache(0), func(context.Context) error { return nil })
}

// TestBootstrapRetryRebuild starts each attempt through the command, including
// its real S3 downloader, with the default --validate=false. A retry must use
// checkpoint volumes as its fold seed even when the abandoned attempt durably
// committed some or all derived delta state.
func TestBootstrapRetryRebuild(t *testing.T) {
	// The EOF case temporarily replaces process stdin, so these sibling cases
	// deliberately run sequentially and this test does not call t.Parallel.
	for _, partial := range []bool{false, true} {
		name := "completed rebuild then confirmation EOF"
		deltaCount := 2
		if partial {
			name = "later rebuild error after committed batch"
			deltaCount = 5001
		}
		t.Run(name, func(t *testing.T) {
			logger := logging.NopZap()
			source, err := dal.NewStore(t.TempDir(), logger, noop.NewMeterProvider().Meter("bootstrap-retry"), dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			batch := source.OpenWriteSession()
			require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("0123456789abcdef0123456789abcdef")))
			require.NoError(t, batch.Commit())
			adm := newBootstrapRetryAdmission(t, source)
			ctx := internalauth.WithSystemActor(t.Context(), commands.ComponentClusterPolicy)
			_, err = adm.AdmitClusterPolicy(ctx, &commonpb.ClusterPolicy{
				Revision: 1, QueryCheckpointLimit: 10,
				MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
				MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
				MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
				MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
				MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
			})
			require.NoError(t, err)
			apply := func(req *servicepb.Request) {
				_, applyErr := adm.Admit(ctx, servicepb.UnsignedApplyRequest("", req))
				require.NoError(t, applyErr)
			}
			apply(actions.CreateLedgerAction("retry", nil))
			payment := func(amount uint64) *servicepb.Request {
				return actions.CreateTransactionAction("retry", []*commonpb.Posting{{Source: "world", Destination: "alice", Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(amount)}}, nil, nil)
			}
			apply(payment(100))
			requireBootstrapRetryVolumes(t, source, 100)

			// Generated Storage expectations retain the real backup orchestration
			// without adding a second hand-written implementation of its interface.
			var mu sync.Mutex
			objects := map[string][]byte{}
			storage := NewMockStorage(gomock.NewController(t))
			storage.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string, r io.Reader, _ int64) error {
				data, readErr := io.ReadAll(r)
				if readErr != nil {
					return readErr
				}
				mu.Lock()
				objects[key] = data
				mu.Unlock()

				return nil
			}).AnyTimes()
			storage.EXPECT().GetFile(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string) (io.ReadCloser, error) {
				mu.Lock()
				defer mu.Unlock()
				data, ok := objects[key]
				if !ok {
					return nil, backup.ErrFileNotFound
				}

				return io.NopCloser(bytes.NewReader(data)), nil
			}).AnyTimes()
			storage.EXPECT().ListFiles(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, prefix string) ([]string, error) {
				mu.Lock()
				defer mu.Unlock()
				var keys []string
				for key := range objects {
					if strings.HasPrefix(key, prefix) {
						keys = append(keys, key)
					}
				}

				return keys, nil
			}).AnyTimes()
			storage.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string) error {
				mu.Lock()
				defer mu.Unlock()
				delete(objects, key)

				return nil
			}).AnyTimes()
			require.NoError(t, source.Flush())
			_, err = backup.RunBackup(ctx, logger, source, storage, "retry", "full")
			require.NoError(t, err)
			for range deltaCount {
				apply(payment(1))
			}
			require.NoError(t, source.Flush())
			inc, err := backup.RunIncrementalBackup(ctx, logger, source, storage, "retry", 0)
			require.NoError(t, err)
			require.EqualValues(t, deltaCount, inc.LogEntriesExported)
			require.Positive(t, inc.AuditEntriesExported)
			manifest, err := backup.ReadManifest(ctx, storage, backup.ManifestKey("retry"))
			require.NoError(t, err)
			require.NotNil(t, manifest.Checkpoint)
			require.NotEmpty(t, manifest.Exports)
			requireBootstrapRetryVolumes(t, source, uint64(100+deltaCount))

			var corruptKey string
			var corruptData []byte
			if partial {
				for _, segment := range manifest.Exports {
					if segment.Type == "log" && segment.EndSeq == manifest.LastExportLogSequence() {
						corruptKey = segment.Key
						corruptData = corruptBootstrapRetryLastLog(t, objects[segment.Key], segment.EndSeq)
					}
				}
				require.NotEmpty(t, corruptKey)
			}
			downloads := map[string]int{}
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimPrefix(r.URL.Path, "/objects/")
				mu.Lock()
				defer mu.Unlock()
				data, ok := objects[key]
				if !ok {
					http.NotFound(w, r)

					return
				}
				downloads[key]++
				if key == corruptKey && downloads[key] == 1 {
					data = corruptData
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				_, _ = w.Write(data) // The client may close its connection after an injected failure.
			}))
			t.Cleanup(endpoint.Close)
			dataDir := t.TempDir()
			run := func(yes bool) error {
				cmd := NewBootstrapCommand()
				cmd.SilenceUsage = true
				cmd.SilenceErrors = true
				args := []string{"--data-dir", dataDir, "--bucket-id", "retry", "--s3-bucket", "objects", "--s3-region", "us-east-1", "--s3-endpoint", endpoint.URL, "--s3-access-key-id", "test", "--s3-secret-access-key", "test"}
				if yes {
					args = append(args, "--yes")
				}
				cmd.SetArgs(args)

				return cmd.ExecuteContext(t.Context())
			}
			if partial {
				err = run(true)
				require.ErrorContains(t, err, "reading log cursor after 5000 logs")
			} else {
				stdin, openErr := os.Open(os.DevNull)
				require.NoError(t, openErr)
				original := os.Stdin
				os.Stdin = stdin
				err = run(false)
				os.Stdin = original
				require.NoError(t, stdin.Close())
				require.ErrorIs(t, err, io.EOF)
				require.ErrorContains(t, err, "reading confirmation")
			}
			staging := filepath.Join(dataDir, "restore-staging")
			abandoned, err := dal.OpenReadOnly(staging, logger)
			require.NoError(t, err)
			committed := deltaCount
			if partial {
				committed = 5000
			}
			requireBootstrapRetryVolumes(t, abandoned, uint64(100+committed))
			require.NoError(t, abandoned.Close())
			marker, err := node.ReadRestoredMarker(dataDir)
			require.NoError(t, err)
			require.Nil(t, marker)

			require.NoError(t, run(true))
			restored, err := dal.OpenReadOnly(filepath.Join(dataDir, "checkpoints", "0"), logger)
			require.NoError(t, err)
			defer func() { require.NoError(t, restored.Close()) }()
			requireBootstrapRetryVolumes(t, restored, uint64(100+deltaCount))
			var findings []*servicepb.CheckStoreError
			checker := check.NewChecker(restored, attributes.New(), nil, logger)
			require.NoError(t, checker.Check(t.Context(), func(event *servicepb.CheckStoreEvent) {
				if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
					findings = append(findings, e.Error)
				}
			}))
			require.Empty(t, findings)
			marker, err = node.ReadRestoredMarker(dataDir)
			require.NoError(t, err)
			require.NotNil(t, marker)
			_, err = os.Stat(staging)
			require.ErrorIs(t, err, os.ErrNotExist)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, 2, downloads[backup.ManifestKey("retry")])
			for _, file := range manifest.Checkpoint.Files {
				require.Equal(t, 2, downloads[file.Key])
			}
			for _, segment := range manifest.Exports {
				require.Equal(t, 2, downloads[segment.Key])
			}
		})
	}
}

func requireBootstrapRetryVolumes(t *testing.T, store *dal.Store, amount uint64) {
	t.Helper()
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	for _, account := range []string{"alice", "world"} {
		volume, err := attributes.New().Volume.Get(handle, domain.NewVolumeKey("retry", account, "USD/2", "").Bytes())
		require.NoError(t, err)
		require.NotNil(t, volume)
		input, output := uint64(0), amount
		if account == "alice" {
			input, output = amount, 0
		}
		require.Equal(t, commonpb.NewUint256FromUint64(input).ToBigInt(), volume.GetInput().ToBigInt(), "%s input", account)
		require.Equal(t, commonpb.NewUint256FromUint64(output).ToBigInt(), volume.GetOutput().ToBigInt(), "%s output", account)
		require.Equal(t, big.NewInt(int64(input)-int64(output)), new(big.Int).Sub(volume.GetInput().ToBigInt(), volume.GetOutput().ToBigInt()), "%s balance", account)
	}
}

// Preserve the stream framing, key range and exact byte count so export import
// succeeds and the failure occurs in RebuildDelta after its first batch commit.
func corruptBootstrapRetryLastLog(t *testing.T, data []byte, seq uint64) []byte {
	t.Helper()
	raw := bytes.NewReader(data)
	reader := backup.NewKVStreamReader(raw)
	require.NoError(t, reader.ReadHeader())
	var out bytes.Buffer
	writer := backup.NewKVStreamWriter(&out)
	require.NoError(t, writer.WriteHeader())
	corrupted := 0
	for raw.Len() > 4 {
		key, value, err := reader.ReadEntry()
		require.NoError(t, err)
		if binary.BigEndian.Uint64(key[2:]) == seq {
			value = bytes.Repeat([]byte{0xff}, len(value))
			corrupted++
		}
		require.NoError(t, writer.WriteEntry(key, value))
	}
	require.Equal(t, 4, raw.Len(), "the remaining bytes are the footer sentinel")
	require.Equal(t, 1, corrupted)
	require.NoError(t, writer.WriteFooter())
	require.Len(t, out.Bytes(), len(data))

	return out.Bytes()
}
