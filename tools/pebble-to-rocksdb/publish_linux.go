//go:build linux

package main

import "golang.org/x/sys/unix"

func publishExclusive(stage, dest string) error {
	return unix.Renameat2(unix.AT_FDCWD, stage, unix.AT_FDCWD, dest, unix.RENAME_NOREPLACE)
}
