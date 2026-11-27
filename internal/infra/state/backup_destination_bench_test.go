package state

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// BenchmarkActiveDestinationIdentity measures the O(active jobs) admission
// scan without storage or Raft, using representative endpoint sizes.
func BenchmarkActiveDestinationIdentity(b *testing.B) {
	for _, count := range []int{0, 10, 100} {
		b.Run(fmt.Sprintf("jobs-%d", count), func(b *testing.B) {
			jobs := NewBackupJobsState()
			for i := range count {
				jobs.active[fmt.Sprintf("job-%d", i)] = &raftcmdpb.BackupJob{Destination: &raftcmdpb.BackupDestination{
					BucketId: "ledger", Target: &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{
						Bucket: "backups", Endpoint: fmt.Sprintf("https://store-%d.example:9000", i),
					}},
				}}
			}
			candidate := &raftcmdpb.BackupDestination{BucketId: "ledger", Target: &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{
				Bucket: "backups", Endpoint: "https://absent.example:9000",
			}}}
			key := PhysicalDestinationKey(candidate)
			b.ResetTimer()
			for b.Loop() {
				for _, destination := range jobs.ActiveDestinations() {
					if PhysicalDestinationKey(destination) == key {
						b.Fatal("candidate unexpectedly matched")
					}
				}
			}
		})
	}
}
