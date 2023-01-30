package fctl

import (
	"os/exec"
	"runtime"
)

func Map[SRC any, DST any](srcs []SRC, mapper func(SRC) DST) []DST {
	ret := make([]DST, 0)
	for _, src := range srcs {
		ret = append(ret, mapper(src))
	}
	return ret
}

func MapKeys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0)
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func Prepend[V any](array []V, items ...V) []V {
	return append(items, array...)
}

func Open(url string) error {
	var (
		cmd  string
		args []string
	)

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start()
}
