package ledger

import "testing"

func TestStats(t *testing.T) {
	with(func(l *Ledger) {
		_, err := l.Stats()

		if err != nil {

			t.Error(err)
		}
	})
}
