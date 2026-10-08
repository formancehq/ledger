package main

import (
	"testing"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/backupdrivertest"
)

func TestBackupDriverErrorClassification(t *testing.T) {
	backupdrivertest.Run(t, run, backupdrivertest.Stage{
		Operation: "Backup", GRPCMethod: ledgerpb.ClusterService_Backup_FullMethodName,
		NoCheckpointUnexpected: true,
	})
}
