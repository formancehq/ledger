package wal

import (
	"os"
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/wal/waltest"
)

// TestMain shrinks etcd WAL segments for this binary, whose tests open Raft
// WALs. See waltest; its own tests keep the production segment size covered.
func TestMain(m *testing.M) {
	waltest.UseSmallSegments()
	os.Exit(m.Run())
}
