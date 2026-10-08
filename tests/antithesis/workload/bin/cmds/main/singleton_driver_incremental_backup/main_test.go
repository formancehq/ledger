package main

import (
	"testing"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/backupdrivertest"
)

func TestBackupDriverErrorClassification(t *testing.T) {
	backupdrivertest.Run(t, run,
		backupdrivertest.Stage{
			Operation: "Backup (pre-incremental)", GRPCMethod: ledgerpb.ClusterService_Backup_FullMethodName,
			NoCheckpointUnexpected: true,
		},
		backupdrivertest.Stage{
			Operation: "IncrementalBackup", GRPCMethod: ledgerpb.ClusterService_IncrementalBackup_FullMethodName,
			NoCheckpointUnexpected: false,
		},
		backupdrivertest.Stage{
			Operation: "second IncrementalBackup", GRPCMethod: ledgerpb.ClusterService_IncrementalBackup_FullMethodName,
			NoCheckpointUnexpected: false,
		},
	)
}
