package grpc

import (
	"crypto/rand"
	"encoding/hex"
	"maps"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	sessionIDBytes    = 16
	sessionReapPeriod = 60 * time.Second
	defaultSessionTTL = 5 * time.Minute
)

type snapshotSession struct {
	syncName       string
	checkpointPath string
	lastAccess     time.Time
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
	ss.sessions[id] = &snapshotSession{
		syncName:       syncName,
		checkpointPath: checkpointPath,
		lastAccess:     time.Now(),
	}
	ss.mu.Unlock()

	return id, nil
}

func (ss *snapshotSessionStore) get(sessionID string) (*snapshotSession, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	s, ok := ss.sessions[sessionID]
	if ok {
		s.lastAccess = time.Now()
	}

	return s, ok
}

func (ss *snapshotSessionStore) remove(sessionID string) {
	ss.mu.Lock()
	s, ok := ss.sessions[sessionID]
	if ok {
		delete(ss.sessions, sessionID)
	}
	ss.mu.Unlock()

	if ok {
		ss.cleanupCheckpoint(s.syncName)
	}
}

func (ss *snapshotSessionStore) reapLoop() {
	ticker := time.NewTicker(sessionReapPeriod)
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
		expiredSessions = append(expiredSessions, ss.sessions[id])
		delete(ss.sessions, id)
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
		}).Errorf("Failed to remove temporary checkpoint for expired session")
	}
}

func (ss *snapshotSessionStore) stop() {
	close(ss.stopCh)

	// Clean up all remaining sessions.
	ss.mu.Lock()
	remaining := make(map[string]*snapshotSession, len(ss.sessions))
	maps.Copy(remaining, ss.sessions)
	ss.sessions = make(map[string]*snapshotSession)
	ss.mu.Unlock()

	for _, s := range remaining {
		ss.cleanupCheckpoint(s.syncName)
	}
}

func generateSessionID() (string, error) {
	b := make([]byte, sessionIDBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}
