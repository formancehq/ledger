package ledger_test

import (
	"testing"

	"github.com/numary/ledger/pkg/ledger"
)

func TestVerify(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		err := l.Verify()

		if err != nil {
			t.Error(err)
		}
	})
}
