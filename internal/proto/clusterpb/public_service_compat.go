package clusterpb

import ledgerpb "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

type ClusterServiceClient = ledgerpb.ClusterServiceClient
type ClusterServiceServer = ledgerpb.ClusterServiceServer
type UnimplementedClusterServiceServer = ledgerpb.UnimplementedClusterServiceServer
type UnsafeClusterServiceServer = ledgerpb.UnsafeClusterServiceServer

var NewClusterServiceClient = ledgerpb.NewClusterServiceClient
var RegisterClusterServiceServer = ledgerpb.RegisterClusterServiceServer

const (
	ClusterService_GetClusterState_FullMethodName            = ledgerpb.ClusterService_GetClusterState_FullMethodName
	ClusterService_GetQueryCheckpointSchedule_FullMethodName = ledgerpb.ClusterService_GetQueryCheckpointSchedule_FullMethodName
	ClusterService_ListQueryCheckpoints_FullMethodName       = ledgerpb.ClusterService_ListQueryCheckpoints_FullMethodName
	ClusterService_IncrementalBackup_FullMethodName          = ledgerpb.ClusterService_IncrementalBackup_FullMethodName
	ClusterService_Backup_FullMethodName                     = ledgerpb.ClusterService_Backup_FullMethodName
)

var ClusterService_ServiceDesc = ledgerpb.ClusterService_ServiceDesc
