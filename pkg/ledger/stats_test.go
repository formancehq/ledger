package ledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	runOnLedger(t, func(l *Ledger) {
		_, err := l.Stats(context.Background())
		assert.NoError(t, err)
	})
}
