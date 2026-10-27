package capabilities

import (
	"strconv"
	"testing"
)

// BenchmarkEvaluate records the deterministic cost shape of the proposal
// gate. The admission path supplies a lock-consistent membership snapshot and
// calls this pure function while holding its proposal/index serialization
// guards. Keep the matrix representative of a small production cluster and a
// larger deployment so changes to the O(members*required) work are visible.
func BenchmarkEvaluate(b *testing.B) {
	for _, size := range []int{3, 7} {
		for _, requiredCount := range []int{0, 1, 4} {
			b.Run(memberCaseName(size, requiredCount), func(b *testing.B) {
				state := State{Activations: make(map[CapabilityID]bool, requiredCount)}
				required := make([]CapabilityID, requiredCount)
				for i := range required {
					id := CapabilityID(i + 1)
					required[i] = id
					state.Activations[id] = true
				}
				members := make([]Member, size)
				for i := range members {
					members[i] = Member{
						NodeID:       uint64(i + 1),
						Role:         Voter,
						Reachable:    true,
						Capabilities: append([]CapabilityID(nil), required...),
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !Evaluate(state, required, false, members).Allowed {
						b.Fatal("representative capability state was rejected")
					}
				}
			})
		}
	}
}

func memberCaseName(memberCount, requiredCount int) string {
	return "members=" + strconv.Itoa(memberCount) + "/required=" + strconv.Itoa(requiredCount)
}
