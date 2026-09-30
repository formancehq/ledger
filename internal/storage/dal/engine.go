package dal

import (
	"fmt"
	"os"
	"sort"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// EngineOpener opens an engine.DB at dir for a main store configured with cfg.
type EngineOpener func(dir string, cfg Config) (engine.DB, error)

// EnginePebble is the default engine name.
const EnginePebble = "pebble"

// engineOpeners holds the alternative engines compiled into this binary.
// Pebble is not listed: it is the default open path in NewStore and needs
// the Pebble-specific options built there. Alternatives register from an
// init() in a build-tagged file (see engine_rocksdb.go).
var engineOpeners = map[string]EngineOpener{}

// RegisterEngine makes an alternative engine selectable through Config.Engine.
func RegisterEngine(name string, open EngineOpener) {
	if _, dup := engineOpeners[name]; dup {
		panic(fmt.Sprintf("dal: engine %q registered twice", name))
	}
	engineOpeners[name] = open
}

// AvailableEngines lists the engine names this binary can open.
func AvailableEngines() []string {
	names := []string{EnginePebble}
	for n := range engineOpeners {
		names = append(names, n)
	}
	sort.Strings(names)

	return names
}

// engineOpener resolves cfg.Engine to a registered opener. It returns
// (nil, false) for Pebble and an error for an engine this binary does not
// include.
func engineOpener(cfg Config) (EngineOpener, bool, error) {
	name := cfg.Engine
	if name == "" || name == EnginePebble {
		return nil, false, nil
	}
	open, ok := engineOpeners[name]
	if !ok {
		return nil, false, fmt.Errorf("storage engine %q is not available in this build (available: %v)", name, AvailableEngines())
	}

	return open, true, nil
}

// TestEngineEnv is the environment variable test helpers honour to run the
// main-store suites on an alternative engine, e.g. LEDGER_TEST_ENGINE=rocksdb
// with -tags rocksdb.
const TestEngineEnv = "LEDGER_TEST_ENGINE"

func engineFromEnv() string {
	return os.Getenv(TestEngineEnv)
}
