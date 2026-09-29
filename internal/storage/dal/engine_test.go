package dal

import "testing"

// skipUnlessPebble skips tests whose mechanics are tied to Pebble (fault
// injection through Pebble files, Pebble-shaped metrics) when the suite runs
// on an alternative engine through LEDGER_TEST_ENGINE.
func skipUnlessPebble(t *testing.T) {
	t.Helper()

	if e := engineFromEnv(); e != "" && e != EnginePebble {
		t.Skipf("Pebble-specific test, running on %s", e)
	}
}
