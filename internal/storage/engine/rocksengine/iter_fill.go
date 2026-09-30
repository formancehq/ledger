//go:build rocksdb

package rocksengine

/*
#include <string.h>
#include <stdint.h>
#include "rocksdb/c.h"

// ledger_iter_fill copies consecutive entries from the iterator's current
// position into buf as [u32 klen][key][u32 vlen][value] records, advancing
// the iterator past each copied entry. It stops when buf is full or the
// iterator turns invalid.
//
// Returns the number of entries copied and sets *used to the bytes written.
// If the first entry alone does not fit, returns -1 and sets *used to the
// size it needs; the iterator is left in place.
static int ledger_iter_fill(rocksdb_iterator_t* it, char* buf, size_t cap, size_t* used) {
	int n = 0;
	size_t off = 0;
	while (rocksdb_iter_valid(it)) {
		rocksdb_slice_t k = rocksdb_iter_key_slice(it);
		rocksdb_slice_t v = rocksdb_iter_value_slice(it);
		size_t need = 8 + k.size + v.size;
		if (off + need > cap) {
			if (n == 0) { *used = need; return -1; }
			break;
		}
		uint32_t kl = (uint32_t)k.size, vl = (uint32_t)v.size;
		memcpy(buf + off, &kl, 4); off += 4;
		memcpy(buf + off, k.data, k.size); off += k.size;
		memcpy(buf + off, &vl, 4); off += 4;
		memcpy(buf + off, v.data, v.size); off += v.size;
		n++;
		rocksdb_iter_next(it);
	}
	*used = off;
	return n;
}
*/
import "C"

import (
	"sync"
	"unsafe"

	"github.com/linxGnu/grocksdb"
)

// iterFillBufferSize is the batch buffer handed to ledger_iter_fill. With
// ledger-sized entries (~130 B) one full cgo call yields ~500 keys.
// iterFillInitialWindow is the budget of the first fill after a seek: a
// point-like seek copies a handful of entries, long scans double the
// window on every refill.
const (
	iterFillBufferSize    = 64 << 10
	iterFillInitialWindow = 2 << 10
)

// iterBufPool recycles fill buffers across iterators; a 64 KiB allocation
// per NewIter showed up as the dominant allocation of short scans.
var iterBufPool = sync.Pool{New: func() any { return make([]byte, iterFillBufferSize) }}

// rawIterator returns the C handle behind a grocksdb.Iterator. grocksdb keeps
// it unexported; the struct's first field is the pointer (grocksdb v1.11.0,
// iterator.go). Pinned by the go.mod version, checked by the conformance
// suite.
func rawIterator(it *grocksdb.Iterator) *C.rocksdb_iterator_t {
	return *(**C.rocksdb_iterator_t)(unsafe.Pointer(it))
}

// fill runs one batched copy into buf. It returns the entries copied, the
// bytes used, and whether the buffer must grow to hold the next entry.
func fill(it *grocksdb.Iterator, buf []byte) (n int, used int, grow int) {
	var cUsed C.size_t
	r := C.ledger_iter_fill(rawIterator(it), (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &cUsed)
	if r < 0 {
		return 0, 0, int(cUsed)
	}

	return int(r), int(cUsed), 0
}
