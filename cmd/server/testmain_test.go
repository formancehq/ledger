package server

import (
	"os"
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/wal/waltest"
)

// TestMain shrinks etcd WAL segments for this binary and for the server
// processes its tests re-execute from it, which open Raft WALs. See waltest.
func TestMain(m *testing.M) {
	waltest.UseSmallSegments()
	os.Exit(m.Run())
}
