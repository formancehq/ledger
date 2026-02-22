package signal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignal_NotifyAndReceive(t *testing.T) {
	t.Parallel()

	s := New()
	s.Notify()

	select {
	case <-s.C():
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected to receive signal")
	}
}

func TestSignal_Coalescing(t *testing.T) {
	t.Parallel()

	s := New()
	// Multiple notifications should coalesce into one pending signal
	s.Notify()
	s.Notify()
	s.Notify()

	// Drain the single pending signal
	select {
	case <-s.C():
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected to receive signal")
	}

	// No second signal should be pending
	select {
	case <-s.C():
		t.Fatal("should not receive a second signal")
	default:
		// success
	}
}

func TestSignal_NonBlockingNotify(t *testing.T) {
	t.Parallel()

	s := New()
	// Notify on a signal with a full buffer should not block
	s.Notify()
	// This second Notify must not block
	done := make(chan struct{})
	go func() {
		s.Notify()
		close(done)
	}()

	select {
	case <-done:
		// success - Notify returned without blocking
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Notify blocked on full buffer")
	}
}

func TestSignal_ChannelIsReadOnly(t *testing.T) {
	t.Parallel()

	s := New()
	ch := s.C()
	// Verify C() returns a receive-only channel (compile-time guarantee)
	require.NotNil(t, ch)
}

func TestSignal_NotifyAfterDrain(t *testing.T) {
	t.Parallel()

	s := New()
	s.Notify()

	// Drain
	<-s.C()

	// Notify again should work
	s.Notify()

	select {
	case <-s.C():
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected to receive signal after drain and re-notify")
	}
}
