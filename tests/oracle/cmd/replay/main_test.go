package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestParseRequiresOutcome(t *testing.T) {
	t.Parallel()

	raw, err := (&servicepb.ApplyRequest{SkipResponse: true}).MarshalVT()
	require.NoError(t, err)
	encoded := base64.StdEncoding.EncodeToString(raw)
	path := filepath.Join(t.TempDir(), "dump.log")
	content := "[batch-dump] seq=1 b64=" + encoded + "\n" +
		"[batch-dump] ticket=2 seq=2 outcome=OK b64=" + encoded + "\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	batches, err := parse(path)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.EqualValues(t, 2, batches[0].ticket)
	require.Equal(t, "OK", batches[0].outcome)
}
