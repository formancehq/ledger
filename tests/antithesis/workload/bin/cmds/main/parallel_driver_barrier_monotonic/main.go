package main

import (
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The high-water mark persists the largest commit index returned by any
// successful Barrier, across invocations of this driver within a timeline.
// Linearizability makes it a valid floor: any value already in the file was
// written by a barrier that completed before the current call started. A
// missing or torn file yields no floor — weaker check, never a false failure.
const hwmPath = "/tmp/barrier-commit-index-hwm"

// driverTimeout bounds one invocation; the gRPC client already retries
// UNAVAILABLE internally, so hitting this deadline means the SUT hung.
const driverTimeout = 2 * time.Minute

func main() {
	log.Println("composer: parallel_driver_barrier_monotonic")

	ctx, cancel := context.WithTimeout(context.Background(), driverTimeout)
	defer cancel()

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)

		return
	}
	defer conn.Close()

	clusterClient := clusterpb.NewClusterServiceClient(conn)

	// Shape axis: vary the per-invocation barrier count across timelines so
	// some timelines probe a single barrier and others a long session.
	// Menu axis not applicable: BarrierRequest carries no parameters.
	iterations := antirandom.RandomChoice([]int{1, 2, 8, 32})

	var (
		prev          uint64 // last successful commit index in this session
		hadSuccess    bool
		prevLeader    uint64
		leaderChanged bool
	)

	for i := 0; i < iterations && ctx.Err() == nil; i++ {
		// Occasionally observe the leader so that successes landing right
		// after a change steer exploration toward the forwarding-during-
		// election window where a deposed leader could answer.
		if internal.Rand().Uint64()%2 == 0 {
			if state, stateErr := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{}); stateErr == nil {
				leader := uint64(state.GetLeader())
				if prevLeader != 0 && leader != 0 && leader != prevLeader {
					leaderChanged = true
				}
				if leader != 0 {
					prevLeader = leader
				}
			}
		}

		// Must be read BEFORE issuing the barrier: only values written by
		// barriers that completed before this call starts are valid floors.
		floor, hasFloor := readHWM()

		resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
		if err != nil {
			if internal.IsTransient(err) || ctx.Err() != nil {
				continue
			}

			assert.Unreachable("barrier monotonic driver got non-transient error", internal.Details{
				"error":     err,
				"iteration": i,
			})

			return
		}

		ci := resp.GetCommitIndex()
		details := internal.Details{
			"commitIndex":    ci,
			"previous":       prev,
			"floor":          floor,
			"iteration":      i,
			"observedLeader": prevLeader,
		}

		assert.AlwaysGreaterThan(ci, uint64(0), "barrier commit index is positive on success", details)

		if hadSuccess {
			assert.AlwaysGreaterThanOrEqualTo(ci, prev,
				"barrier commit index is monotonic within a session", details)
		}

		if hasFloor {
			assert.AlwaysGreaterThanOrEqualTo(ci, floor,
				"barrier commit index never regresses below the cross-invocation high-water mark", details)
		}

		assert.Sometimes(leaderChanged, "barrier succeeded after an observed leadership change", details)

		prev = ci
		hadSuccess = true

		updateHWM(ci)
	}
}

// readHWM returns the persisted high-water mark, holding a shared flock so a
// concurrent updateHWM cannot be observed mid-write.
func readHWM() (uint64, bool) {
	f, err := os.Open(hwmPath)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }() // read-only handle, close error carries no signal

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return 0, false
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	data, err := io.ReadAll(f)
	if err != nil {
		return 0, false
	}

	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		// Torn write from a killed invocation: drop the floor rather than
		// asserting against garbage.
		return 0, false
	}

	return v, true
}

// updateHWM raises the persisted high-water mark to ci if it is larger,
// holding an exclusive flock to serialize concurrent driver invocations.
func updateHWM(ci uint64) {
	f, err := os.OpenFile(hwmPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		log.Printf("hwm: open failed: %s", err)

		return
	}
	defer func() { _ = f.Close() }() // best-effort persistence, close error carries no signal

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		log.Printf("hwm: lock failed: %s", err)

		return
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	data, err := io.ReadAll(f)
	if err != nil {
		return
	}

	if current, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err == nil && current >= ci {
		return
	}

	if err := f.Truncate(0); err != nil {
		return
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return
	}
	if _, err := f.WriteString(strconv.FormatUint(ci, 10)); err != nil {
		log.Printf("hwm: write failed: %s", err)
	}
}
