//go:build !linux

package diskusage

// mmapRSSBytes is a no-op on non-Linux platforms where /proc/self/smaps is
// not available.
func mmapRSSBytes(_ string) (int64, error) {
	return 0, nil
}
