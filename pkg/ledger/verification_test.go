package ledger

import (
	"testing"
)

func TestVerify(t *testing.T) {
	with(func(l *Ledger) {
		err := l.Verify()

		if err != nil {
			t.Error(err)
		}
	})
}
