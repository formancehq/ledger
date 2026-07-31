package balancehistoryarchive

import (
	"context"
	"errors"
	"time"
)

// ErrReclamationUnsupported means the configured ColdStorage adapter does not
// expose the optional lifecycle companion. Archive reads and writes remain
// available; callers must not attempt remote garbage collection.
var ErrReclamationUnsupported = errors.New("balance history archive reclamation is unsupported")

// RemoteObject is one canonical object owned by this archive Store.
type RemoteObject struct {
	SHA256       [32]byte
	Size         int64
	LastModified time.Time
}

// RemoteObjectPage is one bounded owned-namespace page. NextCursor is opaque
// and empty after the final page.
type RemoteObjectPage struct {
	Objects    []RemoteObject
	NextCursor string
}

// Reclaimer is the optional destructive companion to Archive. It deliberately
// stays separate so ordinary Archive implementations and mocks do not acquire
// deletion authority.
type Reclaimer interface {
	Namespace() string
	List(ctx context.Context, cursor string, limit int) (RemoteObjectPage, error)
	Delete(ctx context.Context, digest [32]byte) error
}
