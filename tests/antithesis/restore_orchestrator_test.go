package antithesis_test

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRestoreFinalizeRequiresDurableProof exercises the exact ambiguity that
// caused EN-3029: the same FailedPrecondition is returned after a restart both
// when finalization committed (the response was lost) and when it did not.
// The orchestrator must accept only the former, using the durable marker and
// checkpoint rather than the error text.
func TestRestoreFinalizeRequiresDurableProof(t *testing.T) {
	t.Parallel()

	script, err := filepath.Abs(filepath.Join("workload", "restore-orchestrator.sh"))
	require.NoError(t, err)
	require.FileExists(t, script)

	for _, tc := range []struct {
		name      string
		proof     string
		commandOK string
		wantPass  bool
	}{
		{name: "failed before commit", proof: "absent", commandOK: "no", wantPass: false},
		{name: "lost response after commit", proof: "present", commandOK: "no", wantPass: true},
		{name: "success without commit", proof: "absent", commandOK: "yes", wantPass: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			probe := `
source "$RESTORE_SCRIPT"
POD_PREFIX=ledger-test
k() {
  if [ "$1" != exec ]; then return 1; fi
  if [ "$PROOF" = present ]; then
    printf '{"lastAppliedIndex":42,"lastAppliedTimestamp":1}\n'
    return 0
  fi
  return 1
}
exec_ledgerctl() {
  if [ "$COMMAND_OK" = yes ]; then return 0; fi
  printf 'failed to preview restore: no backup downloaded; download a backup first\n'
  return 1
}
finalize_step
`
			cmd := exec.Command("bash", "-e", "-c", probe)
			cmd.Env = append(cmd.Environ(),
				"MODEL_RESTORE_REQ=/tmp/restore-request",
				"MODEL_RESTORE_RESP=/tmp/restore-response",
				"RESTORE_SCRIPT="+script,
				"PROOF="+tc.proof,
				"COMMAND_OK="+tc.commandOK,
			)
			err := cmd.Run()
			if tc.wantPass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
