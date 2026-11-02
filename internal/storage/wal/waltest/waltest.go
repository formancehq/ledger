// Package waltest shrinks the etcd WAL segment size for test binaries.
//
// etcd preallocates every WAL segment at its full size and keeps one spare
// segment preallocated ahead of the writer, so each Raft WAL a test opens
// reserves two 64 MB segments. On APFS those preallocations reach the disk as
// physical writes and survive until the test's TempDir is removed: a local
// race run of the test binaries that open WALs wrote about 40 GB to disk from
// under 1 GB of WAL data and peaked at several GiB of temporary space.
//
// The segment size lives in a process-wide etcd variable that Save reads on
// every append and that each file pipeline captures when created, so it is set
// once per test binary, from TestMain before m.Run, and never changed while a
// WAL may be open. Production code must not import this package.
package waltest

import (
	"flag"

	etcdwal "go.etcd.io/etcd/server/v3/storage/wal"
)

// SegmentSizeBytes is the etcd WAL segment size that UseSmallSegments
// installs. It is small enough that a test crossing several segments writes a
// few MiB, and still larger than the entries most tests append; etcd cuts a
// segment after the write that crosses the threshold, so larger entries remain
// valid.
const SegmentSizeBytes int64 = 1 << 20

// UseSmallSegments sets the etcd WAL segment size to SegmentSizeBytes for the
// whole test binary. Call it from TestMain before m.Run. Binaries that
// re-execute themselves as helper processes call it again in the child through
// the same TestMain.
//
// A run with -test.bench keeps the production segment size so benchmark
// results stay comparable with production and with earlier runs.
func UseSmallSegments() {
	if !flag.Parsed() {
		flag.Parse()
	}
	if bench := flag.Lookup("test.bench"); bench != nil && bench.Value.String() != "" {
		return
	}
	etcdwal.SegmentSizeBytes = SegmentSizeBytes
}
