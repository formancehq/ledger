package state

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type blockingSealerChapterState struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSealerChapterState) ClosingChapters() []*commonpb.Chapter {
	s.once.Do(func() { close(s.entered) })
	<-s.release

	return nil
}

func (*blockingSealerChapterState) ClosingChapterByID(uint64) (*commonpb.Chapter, bool) {
	return nil, false
}

func requireStopJoinsReconciliation(
	t *testing.T,
	start func(),
	stop func(),
	stopCh <-chan struct{},
	reconciliationEntered <-chan struct{},
	releaseReconciliation chan struct{},
) {
	t.Helper()

	start()
	<-reconciliationEntered

	stopReturned := make(chan struct{})
	go func() {
		stop()
		close(stopReturned)
	}()

	<-stopCh

	select {
	case <-stopReturned:
		close(releaseReconciliation)
		t.Fatal("Stop returned while owned reconciliation work was still active")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseReconciliation)
	require.Eventually(t, func() bool {
		select {
		case <-stopReturned:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond, "Stop did not return after reconciliation completed")
}

func TestArchiverStopJoinsReconciliation(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	entered := make(chan struct{})
	release := make(chan struct{})
	var reconcileOnce sync.Once
	archiver := NewArchiver(
		logger,
		nil,
		nil,
		worker.NewChannel[ArchiveRequest](logger, "test-archive", 1),
		func(uint64, []byte) error { return nil },
		func() bool { return true },
		newArchivingChapterState(t),
		"test-bucket",
		func(<-chan struct{}) {
			reconcileOnce.Do(func() {
				close(entered)
				<-release
			})
		},
	)
	archiver.reconcileInterval = time.Nanosecond

	requireStopJoinsReconciliation(t, archiver.Start, archiver.Stop, archiver.w.StopCh(), entered, release)
}

func TestSealerStopJoinsReconciliation(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	entered := make(chan struct{})
	release := make(chan struct{})
	chapterState := &blockingSealerChapterState{entered: entered, release: release}
	sealer := NewSealer(
		logger,
		nil,
		attributes.New(),
		worker.NewChannel[SealRequest](logger, "test-seal", 1),
		func(uint64, []byte, []byte) error { return nil },
		func() bool { return true },
		chapterState,
	)

	requireStopJoinsReconciliation(t, sealer.Start, sealer.Stop, sealer.w.StopCh(), entered, release)
}

func requireConcurrentStop(t *testing.T, start, stop func()) {
	t.Helper()

	start()

	var callers sync.WaitGroup
	callers.Add(2)

	for range 2 {
		go func() {
			defer callers.Done()
			stop()
		}()
	}

	done := make(chan struct{})
	go func() {
		callers.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent Stop calls did not return")
	}
}

func TestArchiverStopIsConcurrentAndIdempotent(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	archiver := NewArchiver(
		logger,
		nil,
		nil,
		worker.NewChannel[ArchiveRequest](logger, "test-archive", 1),
		func(uint64, []byte) error { return nil },
		func() bool { return true },
		newArchivingChapterState(t),
		"test-bucket",
		func(<-chan struct{}) {},
	)
	archiver.reconcileInterval = time.Hour

	requireConcurrentStop(t, archiver.Start, archiver.Stop)
}

func TestSealerStopIsConcurrentAndIdempotent(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	sealer := NewSealer(
		logger,
		nil,
		attributes.New(),
		worker.NewChannel[SealRequest](logger, "test-seal", 1),
		func(uint64, []byte, []byte) error { return nil },
		func() bool { return true },
		newFixedChapterState(nil),
	)
	sealer.reconcileInterval = time.Hour

	requireConcurrentStop(t, sealer.Start, sealer.Stop)
}
