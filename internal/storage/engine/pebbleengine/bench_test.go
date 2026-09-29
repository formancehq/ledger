package pebbleengine_test

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
	"github.com/formancehq/ledger/v3/internal/storage/engine/enginetest"
	"github.com/formancehq/ledger/v3/internal/storage/engine/pebbleengine"
)

func BenchmarkEngine(b *testing.B) {
	enginetest.RunBenchmarks(b, func(dir string, o engine.Options) (engine.DB, error) {
		return pebbleengine.Open(dir, o)
	})
}
