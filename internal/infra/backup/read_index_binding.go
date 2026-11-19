package backup

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
)

// readIndexBinding commits to both file inventories in order. It detects a
// manifest assembled from different full backups even when their history heads
// coincide. It does not authenticate the manifest against a malicious rewrite.
func readIndexBinding(primary, secondary map[string]CheckpointFile) string {
	hash := sha256.New()
	// Buffer length fields and strings together: a full can name 100,000
	// SSTs per store. The underlying SHA-256 writer never returns an error.
	w := bufio.NewWriter(hash)
	var data [8]byte
	writeUint64 := func(n uint64) {
		binary.BigEndian.PutUint64(data[:], n)
		_, _ = w.Write(data[:])
	}
	writeString := func(s string) {
		writeUint64(uint64(len(s)))
		_, _ = w.WriteString(s)
	}

	writeString("ledger.readIndex.v1")
	for _, files := range []map[string]CheckpointFile{primary, secondary} {
		writeUint64(uint64(len(files)))
		names := slices.AppendSeq(make([]string, 0, len(files)), maps.Keys(files))
		slices.Sort(names)
		for _, name := range names {
			cf := files[name]
			writeString(name)
			writeUint64(uint64(cf.Size))
			writeString(cf.Key)
		}
	}

	_ = w.Flush() // hash.Write cannot fail.

	return hex.EncodeToString(hash.Sum(nil))
}

func (cp *CheckpointManifest) verifyReadIndexBinding() error {
	if cp.ReadIndex.InventorySHA256 != readIndexBinding(cp.Files, cp.ReadIndex.Files) {
		return fmt.Errorf("%w: read index and primary inventory binding mismatch", ErrCheckpointFileCorrupted)
	}

	return nil
}
