package grpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"

	clusterpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestRPCAuthPoliciesCoverEveryPublicServiceMethod(t *testing.T) {
	t.Parallel()

	descriptors := []*ggrpc.ServiceDesc{
		&clusterpb.BucketService_ServiceDesc,
		&clusterpb.ClusterService_ServiceDesc,
	}

	methodCount := 0
	for _, descriptor := range descriptors {
		for _, method := range descriptor.Methods {
			assertKnownPolicy(t, descriptor.ServiceName, method.MethodName)
			methodCount++
		}

		for _, stream := range descriptor.Streams {
			assertKnownPolicy(t, descriptor.ServiceName, stream.StreamName)
			methodCount++
		}
	}

	require.Len(t, clusterpb.AllRPCAuthPolicies(), methodCount)
}

func TestRPCAuthPoliciesPinExceptionalMethods(t *testing.T) {
	t.Parallel()

	policies := clusterpb.AllRPCAuthPolicies()

	publicMethods := map[string]bool{}
	dynamicMethods := map[string]clusterpb.DynamicAuthResolver{}
	fixedMethods := map[string]clusterpb.AuthScope{}
	for method, policy := range policies {
		switch policy.GetPolicy().(type) {
		case *clusterpb.MethodAuthPolicy_Public:
			publicMethods[method] = policy.GetPublic()
		case *clusterpb.MethodAuthPolicy_DynamicResolver:
			dynamicMethods[method] = policy.GetDynamicResolver()
		case *clusterpb.MethodAuthPolicy_FixedScope:
			require.NotEqual(t, clusterpb.AuthScope_AUTH_SCOPE_UNSPECIFIED, policy.GetFixedScope(), method)
			fixedMethods[method] = policy.GetFixedScope()
		default:
			require.Failf(t, "missing policy", "method %s has no authentication policy", method)
		}
	}

	require.Equal(t, map[string]bool{
		"/ledger.BucketService/Discovery": true,
	}, publicMethods)
	require.Equal(t, map[string]clusterpb.DynamicAuthResolver{
		"/ledger.BucketService/Apply":               clusterpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY,
		"/ledger.BucketService/GetIndex":            clusterpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX,
		"/ledger.BucketService/GetIndexEntryStatus": clusterpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX_ENTRY_STATUS,
		"/ledger.BucketService/ListIndexes":         clusterpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_LIST_INDEXES,
	}, dynamicMethods)
	require.Equal(t, expectedFixedRPCAuthPolicies(), fixedMethods)
}

func expectedFixedRPCAuthPolicies() map[string]clusterpb.AuthScope {
	return map[string]clusterpb.AuthScope{
		"/cluster.ClusterService/AddLearner":                 clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/Backup":                     clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactPrimary":             clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactSecondary":           clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CreateCheckpoint":           clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/GetClusterState":            clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetDiskUsage":               clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetNodeTime":                clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointInfo":     clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointSchedule": clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/IncrementalBackup":          clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/ListQueryCheckpoints":       clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/PromoteLearner":             clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/RemoveNode":                 clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/TransferLeadership":         clusterpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/ledger.BucketService/AggregateVolumes":             clusterpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeAccounts":              clusterpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeTransactions":          clusterpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/Barrier":                      clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/CheckStore":                   clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ExecutePreparedQuery":         clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetAccount":                   clusterpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetAuditEntry":                clusterpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/GetEventsSinks":               clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetIndexStatus":               clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetLedger":                    clusterpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLedgerStats":               clusterpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLog":                       clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetMetadataSchemaStatus":      clusterpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetNumscript":                 clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetPrimaryMetrics":            clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetSecondaryMetrics":          clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetTemplateUsage":             clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetTransaction":               clusterpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/InspectIndex":                 clusterpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListAccounts":                 clusterpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/ListAuditEntries":             clusterpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/ListLedgers":                  clusterpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListLogs":                     clusterpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListNumscriptVersions":        clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListNumscripts":               clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListPreparedQueries":          clusterpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListSigningKeys":              clusterpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ListTransactions":             clusterpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
	}
}

func TestRPCAuthPolicyLookupFailsClosedForUnknownMethod(t *testing.T) {
	t.Parallel()

	policy, err := clusterpb.RPCAuthPolicyForMethod("/ledger.BucketService/Unknown")

	require.Nil(t, policy)
	require.ErrorAs(t, err, new(*clusterpb.UnknownRPCMethodError))
}

func assertKnownPolicy(t *testing.T, serviceName, methodName string) {
	t.Helper()

	fullMethod := "/" + serviceName + "/" + methodName
	policy, err := clusterpb.RPCAuthPolicyForMethod(fullMethod)
	require.NoError(t, err, fullMethod)
	require.NotNil(t, policy, fullMethod)
}
