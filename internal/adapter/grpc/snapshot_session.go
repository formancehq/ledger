package grpc

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	sessionIDBytes      = 16
	sessionReapInterval = 60 * time.Second
	defaultSessionTTL   = 5 * time.Minute
)

type snapshotSession struct {
	syncName       string
	checkpointPath string
	lastAccess     time.Time
	activeUsers    int
	retired        bool
}

// snapshotSessionStore manages snapshot sessions with TTL-based expiry.
// Each session holds a reference to a temporary Pebble checkpoint.
type snapshotSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*snapshotSession
	store    *dal.Store
	logger   logging.Logger
	ttl      time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
	stopped  bool
}

func newSnapshotSessionStore(store *dal.Store, logger logging.Logger, ttl time.Duration) *snapshotSessionStore {
	ss := &snapshotSessionStore{
		sessions: make(map[string]*snapshotSession),
		store:    store,
		logger:   logger,
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}

	go ss.reapLoop()

	return ss
}

func (ss *snapshotSessionStore) create(syncName, checkpointPath string) (string, error) {
	id, err := generateSessionID()
	if err != nil {
		return "", err
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	if ss.stopped {
		return "", errors.New("snapshot session store stopped")
	}

	ss.sessions[id] = &snapshotSession{
		syncName:       syncName,
		checkpointPath: checkpointPath,
		lastAccess:     time.Now(),
	}

	return id, nil
}

// acquire pins a session's checkpoint until release is called. A retired
// session cannot be acquired again, but its active users may finish.
func (ss *snapshotSessionStore) acquire(sessionID string) (*snapshotSession, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	s, ok := ss.sessions[sessionID]
	if ok {
		s.activeUsers++
		s.lastAccess = time.Now()
	}

	return s, ok
}

func (ss *snapshotSessionStore) release(s *snapshotSession) {
	ss.mu.Lock()
	if s.activeUsers == 0 {
		ss.mu.Unlock()
		panic("releasing snapshot session without an active user")
	}

	s.activeUsers--
	cleanup := s.retired && s.activeUsers == 0
	ss.mu.Unlock()

	if cleanup {
		ss.cleanupCheckpoint(s.syncName)
	}
}

func (ss *snapshotSessionStore) remove(sessionID string) {
	ss.mu.Lock()
	s, ok := ss.sessions[sessionID]
	if ok {
		delete(ss.sessions, sessionID)
		s.retired = true
	}
	cleanup := ok && s.activeUsers == 0
	ss.mu.Unlock()

	if cleanup {
		ss.cleanupCheckpoint(s.syncName)
	}
}

func (ss *snapshotSessionStore) reapLoop() {
	ticker := time.NewTicker(sessionReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ss.stopCh:
			return
		case <-ticker.C:
			ss.reapExpired()
		}
	}
}

func (ss *snapshotSessionStore) reapExpired() {
	now := time.Now()

	ss.mu.Lock()
	var expired []string

	for id, s := range ss.sessions {
		if now.Sub(s.lastAccess) > ss.ttl {
			expired = append(expired, id)
		}
	}

	expiredSessions := make([]*snapshotSession, 0, len(expired))

	for _, id := range expired {
		s := ss.sessions[id]
		delete(ss.sessions, id)
		s.retired = true

		if s.activeUsers == 0 {
			expiredSessions = append(expiredSessions, s)
		}
	}

	ss.mu.Unlock()

	for _, s := range expiredSessions {
		ss.logger.WithFields(map[string]any{
			"syncName": s.syncName,
		}).Infof("Reaping expired snapshot session")
		ss.cleanupCheckpoint(s.syncName)
	}
}

func (ss *snapshotSessionStore) cleanupCheckpoint(syncName string) {
	if ss.store == nil {
		return
	}

	if err := ss.store.RemoveTemporaryCheckpoint(syncName); err != nil {
		ss.logger.WithFields(map[string]any{
			"error":    err,
			"syncName": syncName,
		}).Errorf("Failed to remove temporary checkpoint for snapshot session")
	}
}

func (ss *snapshotSessionStore) stop() {
	ss.stopOnce.Do(func() {
		close(ss.stopCh)

		// Retire all remaining sessions. Checkpoints with active users are
		// cleaned up by the last release.
		ss.mu.Lock()
		ss.stopped = true
		remaining := make([]*snapshotSession, 0, len(ss.sessions))

		for id, s := range ss.sessions {
			delete(ss.sessions, id)
			s.retired = true

			if s.activeUsers == 0 {
				remaining = append(remaining, s)
			}
		}

		ss.mu.Unlock()

		for _, s := range remaining {
			ss.cleanupCheckpoint(s.syncName)
		}
	})
}

func generateSessionID() (string, error) {
	b := make([]byte, sessionIDBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
