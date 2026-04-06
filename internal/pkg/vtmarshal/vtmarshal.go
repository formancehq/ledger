package vtmarshal

import "sync"

// VTMarshaler is the interface for vtprotobuf-generated messages.
type VTMarshaler interface {
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}

// bufPool holds reusable scratch buffers for MarshalToVT to avoid repeated
// buffer growth allocations in the proposal hot path.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)

		return &b
	},
}

// MarshalCopy marshals msg using a pooled scratch buffer and returns a
// newly allocated byte slice safe for long-term retention (e.g. Raft).
func MarshalCopy(msg VTMarshaler) ([]byte, error) {
	bufp := bufPool.Get().(*[]byte) //nolint:errcheck // pool always returns *[]byte

	size := msg.SizeVT()

	buf := *bufp
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := msg.MarshalToVT(buf)
	if err != nil {
		*bufp = buf
		bufPool.Put(bufp)

		return nil, err
	}

	data := make([]byte, n)
	copy(data, buf[:n])

	*bufp = buf
	bufPool.Put(bufp)

	return data, nil
}
