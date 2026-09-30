package pebbleengine_test

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
	"github.com/formancehq/ledger/v3/internal/storage/engine/enginetest"
	"github.com/formancehq/ledger/v3/internal/storage/engine/pebbleengine"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	enginetest.Run(t, func(dir string, o engine.Options) (engine.DB, error) {
		return pebbleengine.Open(dir, o)
	})
}
