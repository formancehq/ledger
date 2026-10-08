package publicpolicy_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/publicpolicy"
)

func TestRPCAuthPoliciesCoverEveryPublicServiceMethod(t *testing.T) {
	t.Parallel()

	descriptors := []*ggrpc.ServiceDesc{
		&ledgerpb.BucketService_ServiceDesc,
		&ledgerpb.ClusterService_ServiceDesc,
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

	require.Len(t, publicpolicy.AllRPCAuthPolicies(), methodCount)
}

func TestRPCAuthPoliciesPinExceptionalMethods(t *testing.T) {
	t.Parallel()

	policies := publicpolicy.AllRPCAuthPolicies()

	publicMethods := map[string]bool{}
	dynamicMethods := map[string]ledgerpb.DynamicAuthResolver{}
	fixedMethods := map[string]ledgerpb.AuthScope{}
	for method, policy := range policies {
		switch policy.GetPolicy().(type) {
		case *ledgerpb.MethodAuthPolicy_Public:
			publicMethods[method] = policy.GetPublic()
		case *ledgerpb.MethodAuthPolicy_DynamicResolver:
			dynamicMethods[method] = policy.GetDynamicResolver()
		case *ledgerpb.MethodAuthPolicy_FixedScope:
			require.NotEqual(t, ledgerpb.AuthScope_AUTH_SCOPE_UNSPECIFIED, policy.GetFixedScope(), method)
			fixedMethods[method] = policy.GetFixedScope()
		default:
			require.Failf(t, "missing policy", "method %s has no authentication policy", method)
		}
	}

	require.Equal(t, map[string]bool{
		"/ledger.BucketService/Discovery": true,
	}, publicMethods)
	require.Equal(t, map[string]ledgerpb.DynamicAuthResolver{
		"/ledger.BucketService/Apply":               ledgerpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY,
		"/ledger.BucketService/GetIndex":            ledgerpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX,
		"/ledger.BucketService/GetIndexEntryStatus": ledgerpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX_ENTRY_STATUS,
		"/ledger.BucketService/ListIndexes":         ledgerpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_LIST_INDEXES,
	}, dynamicMethods)
	require.Equal(t, expectedFixedRPCAuthPolicies(), fixedMethods)
}

func expectedFixedRPCAuthPolicies() map[string]ledgerpb.AuthScope {
	return map[string]ledgerpb.AuthScope{
		"/cluster.ClusterService/AddLearner":                 ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/Backup":                     ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactPrimary":             ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CompactSecondary":           ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/CreateCheckpoint":           ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/GetClusterState":            ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetDiskUsage":               ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetNodeTime":                ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointInfo":     ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/GetQueryCheckpointSchedule": ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/IncrementalBackup":          ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/ListQueryCheckpoints":       ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_READ,
		"/cluster.ClusterService/PromoteLearner":             ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/RemoveNode":                 ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/cluster.ClusterService/TransferLeadership":         ledgerpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE,
		"/ledger.BucketService/AggregateVolumes":             ledgerpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeAccounts":              ledgerpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/AnalyzeTransactions":          ledgerpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/Barrier":                      ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/CheckStore":                   ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ExecutePreparedQuery":         ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetAccount":                   ledgerpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetAuditEntry":                ledgerpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/GetEventsSinks":               ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetIndexStatus":               ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetLedger":                    ledgerpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLedgerStats":               ledgerpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/GetLog":                       ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetMetadataSchemaStatus":      ledgerpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/GetNumscript":                 ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetPrimaryMetrics":            ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetSecondaryMetrics":          ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/GetTemplateUsage":             ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/GetTransaction":               ledgerpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
		"/ledger.BucketService/InspectIndex":                 ledgerpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListAccounts":                 ledgerpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ,
		"/ledger.BucketService/ListAuditEntries":             ledgerpb.AuthScope_AUTH_SCOPE_AUDIT_READ,
		"/ledger.BucketService/ListLedgers":                  ledgerpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListLogs":                     ledgerpb.AuthScope_AUTH_SCOPE_LEDGER_READ,
		"/ledger.BucketService/ListNumscriptVersions":        ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListNumscripts":               ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListPreparedQueries":          ledgerpb.AuthScope_AUTH_SCOPE_QUERY_READ,
		"/ledger.BucketService/ListSigningKeys":              ledgerpb.AuthScope_AUTH_SCOPE_OPS_READ,
		"/ledger.BucketService/ListTransactions":             ledgerpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ,
	}
}

func TestRPCAuthPolicyLookupFailsClosedForUnknownMethod(t *testing.T) {
	t.Parallel()

	policy, err := publicpolicy.RPCAuthPolicyForMethod("/ledger.BucketService/Unknown")

	require.Nil(t, policy)
	require.ErrorAs(t, err, new(*publicpolicy.UnknownRPCMethodError))
}

func assertKnownPolicy(t *testing.T, serviceName, methodName string) {
	t.Helper()

	fullMethod := "/" + serviceName + "/" + methodName
	policy, err := publicpolicy.RPCAuthPolicyForMethod(fullMethod)
	require.NoError(t, err, fullMethod)
	require.NotNil(t, policy, fullMethod)
}
