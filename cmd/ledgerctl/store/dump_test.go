package store

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestDumpValueRedactsAuditKey(t *testing.T) {
	t.Parallel()

	key := []byte{dal.ZoneGlobal, dal.SubGlobAuditKey}
	secret := []byte("audit-key-canary-0123456789abcdef")
	for _, raw := range []bool{false, true} {
		output := dumpValue(key, secret, raw)
		if output != "[REDACTED]" {
			t.Fatalf("raw=%v: audit key was not redacted: %q", raw, output)
		}
		if strings.Contains(output, string(secret)) || strings.Contains(output, hex.EncodeToString(secret)) {
			t.Fatalf("raw=%v: audit key leaked", raw)
		}
	}

	if got := dumpValue([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, []byte{0x12, 0x34}, true); got != "1234" {
		t.Fatalf("unrelated raw value changed: %q", got)
	}
}
