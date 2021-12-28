package ledger

import (
	"context"
	"testing"
)

func TestStats(t *testing.T) {
	with(func(l *Ledger) {
		_, err := l.Stats(context.Background())

		if err != nil {

			t.Error(err)
		}
	})
}
