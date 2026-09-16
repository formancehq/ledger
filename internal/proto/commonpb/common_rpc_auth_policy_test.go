package commonpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRPCAuthPoliciesCoverEveryPublicServiceMethod(t *testing.T) {
	t.Parallel()

	descriptors := []*ggrpc.ServiceDesc{
		&servicepb.BucketService_ServiceDesc,
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

	require.Len(t, commonpb.AllRPCAuthPolicies(), methodCount)
}

func TestRPCAuthPoliciesPinExceptionalMethods(t *testing.T) {
	t.Parallel()

	policies := commonpb.AllRPCAuthPolicies()

	publicMethods := map[string]bool{}
	dynamicMethods := map[string]commonpb.DynamicAuthResolver{}
	fixedMethods := map[string]commonpb.AuthScope{}
	for method, policy := range policies {
		switch policy.GetPolicy().(type) {
		case *commonpb.MethodAuthPolicy_Public:
			publicMethods[method] = policy.GetPublic()
		case *commonpb.MethodAuthPolicy_DynamicResolver:
			dynamicMethods[method] = policy.GetDynamicResolver()
		case *commonpb.MethodAuthPolicy_FixedScope:
			require.NotEqual(t, commonpb.AuthScope_AUTH_SCOPE_UNSPECIFIED, policy.GetFixedScope(), method)
			fixedMethods[method] = policy.GetFixedScope()
		default:
			require.Failf(t, "missing policy", "method %s has no authentication policy", method)
		}
	}

	require.Equal(t, map[string]bool{
		"/ledger.BucketService/Discovery": true,
	}, publicMethods)
	require.Equal(t, map[string]commonpb.DynamicAuthResolver{
		"/ledger.BucketService/Apply":               commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY,
		"/ledger.BucketService/GetIndex":            commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX,
		"/ledger.BucketService/GetIndexEntryStatus": commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX_ENTRY_STATUS,
		"/ledger.BucketService/ListIndexes":         commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_LIST_INDEXES,
	}, dynamicMethods)
	require.Equal(t, expectedFixedRPCAuthPolicies(), fixedMethods)
}

func expectedFixedRPCAuthPolicies() map[string]commonpb.AuthScope {
	return map[string]commonpb.AuthScope{
		"/cluster.ClusterService/AddLearner":                 commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/Backup":                     commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactPrimary":             commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactSecondary":           commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CreateCheckpoint":           commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/GetClusterState":            commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetDiskUsage":               commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetNodeTime":                commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointInfo":     commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointSchedule": commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/IncrementalBackup":          commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/ListQueryCheckpoints":       commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/PromoteLearner":             commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/RemoveNode":                 commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/TransferLeadership":         commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/ledger.BucketService/AggregateVolumes":             commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeAccounts":              commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeTransactions":          commonpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/Barrier":                      commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/CheckStore":                   commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ExecutePreparedQuery":         commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetAccount":                   commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetAuditEntry":                commonpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/GetEventsSinks":               commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetIndexStatus":               commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetLedger":                    commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLedgerStats":               commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLog":                       commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetMetadataSchemaStatus":      commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetNumscript":                 commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetPrimaryMetrics":            commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetSecondaryMetrics":          commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetTemplateUsage":             commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetTransaction":               commonpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/InspectIndex":                 commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListAccounts":                 commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/ListAuditEntries":             commonpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/ListLedgers":                  commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListLogs":                     commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListNumscriptVersions":        commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListNumscripts":               commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListPreparedQueries":          commonpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListSigningKeys":              commonpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ListTransactions":             commonpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
	}
}

func TestRPCAuthPolicyLookupFailsClosedForUnknownMethod(t *testing.T) {
	t.Parallel()

	policy, err := commonpb.RPCAuthPolicyForMethod("/ledger.BucketService/Unknown")

	require.Nil(t, policy)
	require.ErrorAs(t, err, new(*commonpb.UnknownRPCMethodError))
}

func assertKnownPolicy(t *testing.T, serviceName, methodName string) {
	t.Helper()

	fullMethod := "/" + serviceName + "/" + methodName
	policy, err := commonpb.RPCAuthPolicyForMethod(fullMethod)
	require.NoError(t, err, fullMethod)
	require.NotNil(t, policy, fullMethod)
}
