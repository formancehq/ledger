package ledger_test

import (
	"testing"

	"github.com/formancehq/ledger/pkg/ledger"
)

func TestVerify(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		err := l.Verify()

		if err != nil {
			t.Error(err)
		}
	})
}
