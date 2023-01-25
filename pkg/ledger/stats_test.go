package ledger_test

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		_, err := l.Stats(context.Background())
		assert.NoError(t, err)
	})
}
