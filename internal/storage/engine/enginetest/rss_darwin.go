//go:build darwin

package enginetest

// ru_maxrss is in bytes on macOS.
const rssDivisor = 1e6
