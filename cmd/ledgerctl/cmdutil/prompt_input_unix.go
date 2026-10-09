//go:build !windows

package cmdutil

import (
	"context"
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

func runPrompt(_ context.Context, ask func() error) error { return ask() }

func (input *promptInput) read(buffer []byte) (int, error) {
	fd := int(input.Fd())
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		if err := input.ctx.Err(); err != nil {
			return 0, err
		}
		// Poll before reading so Survey can unwind its terminal-mode defers
		// without a background reader or changes to shared descriptor flags.
		_, err := unix.Poll(poll, 100)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return 0, err
		}
		if err := input.ctx.Err(); err != nil {
			return 0, err
		}
		if poll[0].Revents == 0 {
			continue
		}
		if poll[0].Revents&unix.POLLNVAL != 0 {
			return 0, unix.EBADF
		}

		n, err := unix.Read(fd, buffer)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		if n == 0 && err == nil {
			return 0, io.EOF
		}

		return n, err
	}
}
