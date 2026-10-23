package main

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/backupdrivertest"
)

func TestBackupDriverErrorClassification(t *testing.T) {
	backupdrivertest.Run(t, run, backupdrivertest.Stage{
		Operation: "Backup", GRPCMethod: clusterpb.ClusterService_Backup_FullMethodName,
		NoCheckpointUnexpected: true,
	})
}
