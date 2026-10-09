package cmdutil

import (
	"context"
	"runtime"
	"time"

	"golang.org/x/sys/windows"
)

func runPrompt(ctx context.Context, ask func() error) error {
	// Survey reads console events directly from Fd(), bypassing Read and file
	// deadlines. Cancel only the prompt's locked thread, then join the watcher
	// before releasing the thread or its handle. Survey restores console mode.
	// https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-cancelsynchronousio
	cancelIO := windows.NewLazySystemDLL("kernel32.dll").NewProc("CancelSynchronousIo")
	if err := cancelIO.Find(); err != nil {
		return err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	thread, err := windows.OpenThread(windows.THREAD_TERMINATE, false, windows.GetCurrentThreadId())
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(thread) }()

	finished := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-finished:
			return
		case <-ctx.Done():
		}

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			// Retry if cancellation arrived between two synchronous console reads.
			_, _, _ = cancelIO.Call(uintptr(thread))
			select {
			case <-finished:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		close(finished)
		<-watcherDone
	}()

	if err := ctx.Err(); err != nil {
		return err
	}

	return ask()
}

func (input *promptInput) read(buffer []byte) (int, error) {
	var n int
	err := runPrompt(input.ctx, func() error {
		var err error
		n, err = input.file.Read(buffer)

		return err
	})

	return n, err
}
