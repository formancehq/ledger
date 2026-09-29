//go:build linux

package enginetest

// ru_maxrss is in kilobytes on Linux.
const rssDivisor = 1e3
