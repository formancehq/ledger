package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backupinfra "github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAdmissionDestinationKey(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		azure         bool
		first         string
		second        string
		secondAccount string
		equal         bool
	}{
		{name: "S3 host", first: "http://localhost:9000", second: "http://LOCALHOST:9000", equal: true},
		{name: "S3 scheme", first: "http://localhost:9000", second: "HTTP://localhost:9000", equal: true},
		{name: "AWS default", first: "", second: "HTTPS://S3.US-EAST-1.AMAZONAWS.COM/", equal: true},
		{name: "Azure host", azure: true, first: "http://localhost:10000/acct", second: "HTTP://LOCALHOST:10000/acct/", equal: true},
		{name: "Azure default", azure: true, first: "", second: "HTTPS://ACCT.BLOB.CORE.WINDOWS.NET/", equal: true},
		{name: "Azure account case", azure: true, first: "", second: "", secondAccount: "ACCT", equal: true},
		{name: "Azure explicit account case", azure: true, first: "http://localhost:10000/acct", second: "http://localhost:10000/acct", secondAccount: "ACCT", equal: true},
		{name: "IPv6 address", first: "http://[FE80::AB%25BackupA]:9000", second: "http://[fe80::ab%25BackupA]:9000", equal: true},
		{name: "IPv6 zone", first: "http://[fe80::ab%25BackupA]:9000", second: "http://[fe80::ab%25backupa]:9000"},
		{name: "path", first: "http://localhost/Backup", second: "http://localhost/backup"},
		{name: "Azure path", azure: true, first: "http://localhost/Backup", second: "http://localhost/backup"},
		{name: "userinfo", first: "http://User@localhost", second: "http://user@localhost"},
		{name: "invalid endpoint", first: "http://%invalid/", second: "http://%invalid", equal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := testDestination()
			if test.azure {
				first.Target = &raftcmdpb.BackupDestination_Azure{Azure: &raftcmdpb.AzureBackupTarget{AccountName: "acct", Container: "backups", Endpoint: test.first}}
			} else {
				first.GetS3().Endpoint = test.first
			}
			second := first.CloneVT()
			if test.azure {
				second.GetAzure().Endpoint = test.second
				if test.secondAccount != "" {
					second.GetAzure().AccountName = test.secondAccount
				}
			} else {
				second.GetS3().Endpoint = test.second
			}
			original := first.CloneVT()
			originalSecond := second.CloneVT()
			if test.secondAccount != "" {
				require.NotEqual(t, state.CanonicalDestinationKey(first), state.CanonicalDestinationKey(second))
			}
			if test.equal {
				require.Equal(t, admissionDestinationKey(first), admissionDestinationKey(second))
			} else {
				require.NotEqual(t, admissionDestinationKey(first), admissionDestinationKey(second))
			}
			require.Equal(t, original, first, "admission must not rewrite the replicated destination")
			require.Equal(t, originalSecond, second, "admission must not rewrite the replicated destination")
		})
	}
}

func TestRunFull_EndpointReservationBeforeStart(t *testing.T) {
	t.Parallel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	startErr := errors.New("start rejected before apply")
	proposer := NewMockProposer(gomock.NewController(t))
	attempts := 0
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(func(_ context.Context, _ *raftcmdpb.Proposal) error {
		attempts++
		if attempts == 1 {
			close(entered)
			<-release
		}

		return startErr
	})
	orchestrator := newOrchestrator(proposer, nil)
	first := testDestination()
	first.GetS3().Endpoint = "http://localhost:9000"
	second := first.CloneVT()
	second.GetS3().Endpoint = "HTTP://LOCALHOST:9000"
	var firstErr error
	go func() { defer close(done); _, firstErr = orchestrator.RunFull(t.Context(), first, nil) }()
	t.Cleanup(func() { unblock.Do(func() { close(release) }); <-done })
	<-entered
	_, err := orchestrator.RunFull(t.Context(), second, nil)
	require.ErrorIs(t, err, state.ErrBackupInProgress)
	unblock.Do(func() { close(release) })
	<-done
	require.ErrorIs(t, firstErr, startErr)
	_, err = orchestrator.RunFull(t.Context(), second, nil)
	require.ErrorIs(t, err, startErr)
	require.Empty(t, orchestrator.destinations)
}

func TestRunFull_RejectsRestoredEndpointAliases(t *testing.T) {
	t.Parallel()
	for _, kind := range []raftcmdpb.BackupKind{raftcmdpb.BackupKind_BACKUP_KIND_FULL, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			fixture.dst.GetS3().Endpoint = "HTTP://LOCALHOST:9000"
			batch := fixture.store.OpenWriteSession()
			legacy, started, err := fixture.jobs.Start(batch, 1, kind, &raftcmdpb.BackupOrderStart{JobId: 42, Destination: fixture.dst})
			require.NoError(t, err)
			require.True(t, started)
			require.NoError(t, batch.Commit())
			reader, err := fixture.store.NewDirectReadHandle()
			require.NoError(t, err)
			reloaded := state.NewBackupJobsState()
			require.NoError(t, reloaded.RestoreFromStore(reader))
			require.NoError(t, reader.Close())
			fixture.jobs = reloaded
			fixture.orchestrator.jobs = reloaded
			fixture.cleanup.state = reloaded
			fixture.dst = fixture.dst.CloneVT()
			fixture.dst.GetS3().Endpoint = "http://localhost:9000"
			require.NotEqual(t, legacy.GetDestinationKey(), state.CanonicalDestinationKey(fixture.dst), "released FSM key semantics must remain unchanged")
			_, err = fixture.full()
			require.ErrorIs(t, err, state.ErrBackupInProgress)
			require.Empty(t, fixture.starts, "inherited alias must be rejected before proposal")
			require.Zero(t, fixture.puts)
			require.Zero(t, fixture.deletes)
			require.Equal(t, legacy, reloaded.ActiveByDestination(legacy.GetDestination()))
			fixture.cleanup.tick(t.Context())
			require.Empty(t, reloaded.Snapshot())
			_, err = fixture.full()
			require.NoError(t, err)
			require.Empty(t, reloaded.Snapshot())
		})
	}
}

func TestRunFull_EndpointAliasAfterUncertainProposal(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"start-ack", "lost-complete", "lost-fail"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			fixture := newRecoveryFixture(t)
			fixture.orchestrator.jobs = fixture.jobs
			fixture.dst.GetS3().Endpoint = "http://localhost:9000"
			fixture.fault = fault
			fixture.failPut = fault == "lost-fail"
			fixture.seedLedger(1)
			_, err := fixture.full()
			require.Error(t, err)
			require.Equal(t, 1, fixture.faults)
			require.Len(t, fixture.jobs.Snapshot(), 1)
			fixture.dst = fixture.dst.CloneVT()
			fixture.dst.GetS3().Endpoint = "HTTP://LOCALHOST:9000"
			puts, deletes := fixture.puts, fixture.deletes
			_, err = fixture.full()
			require.ErrorIs(t, err, state.ErrBackupInProgress)
			require.Len(t, fixture.starts, 1)
			require.Equal(t, puts, fixture.puts)
			require.Equal(t, deletes, fixture.deletes)
			fixture.cleanup.tick(t.Context())
			require.Empty(t, fixture.jobs.Snapshot())
			_, err = fixture.full()
			require.NoError(t, err)
			require.Len(t, fixture.starts, 2)
			fixture.assertFinished()
			fixture.assertRestorable(1)
		})
	}
}

type destinationObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
	calls   map[string]int
}

func (objects *destinationObjects) storage(t *testing.T, client string, published func()) *MockStorage {
	t.Helper()
	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string, reader io.Reader, _ int64) error {
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			objects.mu.Lock()
			objects.calls[client]++
			objects.objects[key] = data
			objects.mu.Unlock()
			if published != nil && strings.HasSuffix(key, "/manifest.json") {
				published()
			}

			return nil
		})
	storage.EXPECT().GetFile(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string) (io.ReadCloser, error) {
			objects.mu.Lock()
			defer objects.mu.Unlock()
			objects.calls[client]++
			data, exists := objects.objects[key]
			if !exists {
				return nil, backupinfra.ErrFileNotFound
			}

			return io.NopCloser(bytes.NewReader(data)), nil
		})
	storage.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string) error {
			objects.mu.Lock()
			defer objects.mu.Unlock()
			objects.calls[client]++
			delete(objects.objects, key)

			return nil
		})
	storage.EXPECT().ListFiles(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, prefix string) ([]string, error) {
			objects.mu.Lock()
			defer objects.mu.Unlock()
			objects.calls[client]++
			var keys []string
			for key := range objects.objects {
				if strings.HasPrefix(key, prefix) {
					keys = append(keys, key)
				}
			}

			return keys, nil
		})

	return storage
}

func TestRunFull_EndpointCaseDestinationExclusion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ driver, endpoint, secondAccount string }{
		{"s3", "http://LOCALHOST:9000", ""},
		{"s3", "HTTP://localhost:9000", ""},
		{"azure", "http://LOCALHOST:9000", ""},
		{"azure", "HTTP://localhost:9000", ""},
		{"azure", "", "ACCT"},
		{"azure", "http://localhost:9000", "ACCT"},
	} {
		name := test.driver + "/" + test.endpoint
		if test.secondAccount != "" {
			name = "azure/explicit-account-case"
			if test.endpoint == "" {
				name = "azure/default-account-case"
			}
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newTempStore(t)
			jobs := state.NewBackupJobsState()
			proposer := NewMockProposer(gomock.NewController(t))
			proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(_ context.Context, proposal *raftcmdpb.Proposal) error {
				order := extractFullOrder(proposal)
				if start := order.GetStart(); start != nil {
					_, _, err := jobs.Start(nil, 1, raftcmdpb.BackupKind_BACKUP_KIND_FULL, start)

					return err
				}
				_, err := jobs.Complete(nil, 2, order.GetComplete())

				return err
			})
			orchestrator := newOrchestrator(proposer, store)
			orchestrator.jobs = jobs
			first := testDestination()
			if test.driver == "azure" {
				first.Target = &raftcmdpb.BackupDestination_Azure{Azure: &raftcmdpb.AzureBackupTarget{AccountName: "acct", Container: "backups", Endpoint: "http://localhost:9000"}}
			} else {
				first.GetS3().Endpoint = "http://localhost:9000"
			}
			second := first.CloneVT()
			if test.driver == "azure" {
				second.GetAzure().Endpoint = test.endpoint
				if test.secondAccount != "" {
					if test.endpoint == "" {
						first.GetAzure().Endpoint = ""
					}
					second.GetAzure().AccountName = test.secondAccount
				}
			} else {
				second.GetS3().Endpoint = test.endpoint
			}
			objects := &destinationObjects{objects: map[string][]byte{}, calls: map[string]int{}}
			published, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			firstStorage := objects.storage(t, "first", func() { close(published); <-release })
			secondStorage := objects.storage(t, "second", nil)
			var firstErr error
			go func() {
				defer close(done)
				_, firstErr = orchestrator.RunFull(t.Context(), first, firstStorage)
			}()
			t.Cleanup(func() { unblock.Do(func() { close(release) }); <-done })
			select {
			case <-published:
			case <-done:
				t.Fatalf("first backup stopped before manifest publication: %v", firstErr)
			}
			batch := store.OpenWriteSession()
			require.NoError(t, batch.SetBytes([]byte("en2947-committed-change"), []byte("second checkpoint")))
			require.NoError(t, batch.Commit())
			_, secondErr := orchestrator.RunFull(t.Context(), second, secondStorage)
			objects.mu.Lock()
			secondCalls := objects.calls["second"]
			objects.mu.Unlock()
			independent := second.CloneVT()
			independent.BucketId += "-independent"
			_, independentErr := orchestrator.RunFull(t.Context(), independent, secondStorage)
			require.NoError(t, independentErr, "an unrelated namespace must progress while the first writer is blocked")
			unblock.Do(func() { close(release) })
			<-done
			require.NoError(t, firstErr)
			manifest, err := backupinfra.ReadManifest(t.Context(), secondStorage, backupinfra.ManifestKey(first.GetBucketId()))
			require.NoError(t, err)
			missing := 0
			for _, file := range manifest.Checkpoint.Files {
				reader, readErr := secondStorage.GetFile(t.Context(), file.Key)
				if readErr != nil {
					missing++

					continue
				}
				_, verifyErr := file.CopyVerified(io.Discard, reader)
				require.NoError(t, verifyErr)
				require.NoError(t, reader.Close())
			}
			t.Logf("second backup error=%v, storage calls=%d, final manifest missing objects=%d", secondErr, secondCalls, missing)
			require.ErrorIs(t, secondErr, state.ErrBackupInProgress)
			require.Zero(t, secondCalls, "rejected writer must not upload, publish or prune")
			require.Zero(t, missing)
			_, err = orchestrator.RunFull(t.Context(), second, secondStorage)
			require.NoError(t, err, "the destination must be reusable after completion")
			require.Empty(t, jobs.Snapshot())
		})
	}
}
