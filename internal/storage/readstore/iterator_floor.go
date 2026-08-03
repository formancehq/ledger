package readstore

import "bytes"

// seekFloor caches an exhaustion proof for a forward iterator: a failed
// SeekGE(t) proves no entity >= t exists in the iterator's view. The proof is
// valid only for a CLEAN exhaustion of a snapshot-fixed view: the Pebble view
// is fixed at pebble.Iterator creation (so a proof never goes stale and the
// floor is never cleared), and a seek that failed because of an I/O error
// proves nothing about the view's contents (fail drops it — see #320 for the
// error class). Composite iterators re-seek exhausted children once per merge
// step (AND converge, OR findMin, the NOT child catch-up); the floor turns
// every such re-seek at or above the proven bound into an O(1) comparison
// instead of a Pebble seek.
type seekFloor struct {
	bound []byte
	set   bool
}

// covers reports whether SeekGE(target) is proven empty by a prior failure.
func (f *seekFloor) covers(target []byte) bool {
	return f.set && bytes.Compare(target, f.bound) >= 0
}

// fail records a failed SeekGE(target): no entity >= target exists. err is the
// iterator's storage error: a non-nil err means the seek failed for I/O
// reasons, which proves nothing, so no bound is recorded. The bound is copied
// — target may alias a sibling iterator's Pebble key buffer. Callers only
// reach a real (uncovered) seek with target below the current bound, so
// overwriting always tightens it.
func (f *seekFloor) fail(target []byte, err error) {
	if err != nil {
		return
	}

	f.bound = append(f.bound[:0], target...)
	f.set = true
}

// seekCeil is the reverse-iteration analog of seekFloor: a failed SeekLE(t)
// proves no entity <= t exists. Same clean-exhaustion / snapshot-fixed-view
// precondition.
type seekCeil struct {
	bound []byte
	set   bool
}

// covers reports whether SeekLE(target) is proven empty by a prior failure.
func (c *seekCeil) covers(target []byte) bool {
	return c.set && bytes.Compare(target, c.bound) <= 0
}

// fail records a failed SeekLE(target): no entity <= target exists. A non-nil
// err drops the proof, like seekFloor.fail.
func (c *seekCeil) fail(target []byte, err error) {
	if err != nil {
		return
	}

	c.bound = append(c.bound[:0], target...)
	c.set = true
}
