package admission

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const cronTZChildEnv = "LEDGER_TEST_CRON_TZ_CHILD_SPEC"

var checkpointScheduleTimezoneCases = []struct {
	cron     string
	accepted bool
}{
	{"0 0 * * *", true},
	{"@daily", true},
	{"CRON_TZ=UTC 0 0 * * *", true},
	{"TZ=UTC 0 0 * * *", true},
	{"CRON_TZ=UTC @daily", true},
	{"CRON_TZ=UTC 30 0 0 * * *", true},
	// Resolved through node-local tzdata.
	{"CRON_TZ=Ledger/Test 0 0 * * *", false},
	{"TZ=Ledger/Test 0 0 * * *", false},
	{"CRON_TZ=Ledger/Test @daily", false},
	{"CRON_TZ=Europe/Paris 0 0 * * *", false},
	// Resolved through the node's TZ environment variable.
	{"CRON_TZ=Local 0 0 * * *", false},
	{"CRON_TZ= 0 0 * * *", false},
	{"CRON_TZ=utc 0 0 * * *", false},
	{"CRON_TZ=UTC\t0 0 * * *", false},
	// Panics in the cron parser.
	{"TZ=UTC", false},
	{"CRON_TZ=", false},
}

func TestAdmissionRejectsNodeLocalCheckpointScheduleTimezone(t *testing.T) {
	t.Parallel()

	admission, _ := createTestAdmission(t, createTestStore(t))
	for _, tc := range checkpointScheduleTimezoneCases {
		t.Run(tc.cron, func(t *testing.T) {
			t.Parallel()

			order, err := admission.requestToOrder(context.Background(), &servicepb.Request{
				Type: &servicepb.Request_SetQueryCheckpointSchedule{
					SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: tc.cron},
				},
			}, nil, newBulkOverlay())
			if tc.accepted {
				require.NoError(t, err)
				require.Equal(t, tc.cron, order.GetSystemScoped().GetSetQueryCheckpointSchedule().GetCron())

				return
			}

			require.Nil(t, order)
			var cronErr *domain.ErrInvalidCronExpression
			require.ErrorAs(t, err, &cronErr)
			require.Equal(t, tc.cron, cronErr.Expression)
			var business *domain.BusinessError
			require.ErrorAs(t, err, &business)
			require.Equal(t, domain.ErrReasonInvalidCronExpression, business.Reason())
		})
	}
}

// TestAdmittedCheckpointScheduleParsesIndependentOfNodeTZData parses every
// admitted cron in two processes that differ only in the zoneinfo database
// visible through ZONEINFO, which Go reads once per process.
func TestAdmittedCheckpointScheduleParsesIndependentOfNodeTZData(t *testing.T) {
	if cron, ok := os.LookupEnv(cronTZChildEnv); ok {
		_, err := processing.CronParser.Parse(cron)
		fmt.Println(err)

		return
	}

	t.Parallel()

	withZone := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(withZone, "Ledger"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(withZone, "Ledger", "Test"), minimalUTCTZif(), 0o600))
	withoutZone := t.TempDir()

	// Guards against a vacuous comparison: the fixture zone must change the
	// parse outcome for a zone that admission rejects.
	require.NotEqual(t,
		parseCronInChild(t, "CRON_TZ=Ledger/Test 0 0 * * *", withZone),
		parseCronInChild(t, "CRON_TZ=Ledger/Test 0 0 * * *", withoutZone),
	)

	for _, tc := range checkpointScheduleTimezoneCases {
		if !tc.accepted {
			continue
		}
		require.Equal(t, "<nil>", parseCronInChild(t, tc.cron, withZone), tc.cron)
		require.Equal(t, "<nil>", parseCronInChild(t, tc.cron, withoutZone), tc.cron)
	}
}

func parseCronInChild(t *testing.T, cron, zoneinfo string) string {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestAdmittedCheckpointScheduleParsesIndependentOfNodeTZData$")
	cmd.Env = append(os.Environ(), cronTZChildEnv+"="+cron, "ZONEINFO="+zoneinfo)
	out, err := cmd.Output()
	require.NoError(t, err)

	// The test binary prints its own PASS line after the child's output.
	outcome, _, _ := strings.Cut(string(out), "\n")

	return outcome
}

// minimalUTCTZif returns a version-1 TZif file with a single UTC zone type.
func minimalUTCTZif() []byte {
	var buf bytes.Buffer
	buf.WriteString("TZif")
	buf.Write(make([]byte, 16))
	// isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt
	for _, n := range []uint32{0, 0, 0, 0, 1, 4} {
		_ = binary.Write(&buf, binary.BigEndian, n) // bytes.Buffer writes cannot fail
	}
	buf.Write([]byte{0, 0, 0, 0, 0, 0}) // utoff=0, isdst=0, abbrind=0
	buf.WriteString("UTC\x00")

	return buf.Bytes()
}
