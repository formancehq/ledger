package ledgerv3

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"

const (
	bucketServiceName   = "BucketService"
	bucketServicePrefix = "/ledger.BucketService/"
)

func bucketFullMethod(name string) string {
	return bucketServicePrefix + name
}

// Granular Ledger v3 scopes, verbatim from internal/adapter/auth/scopes.go at
// 9a6fa7d0.
//
// RFC 0014 asks for the exact scope of an operation. For Ledger v3 only the
// granular scope is source-derivable: internal/adapter/auth/scope_mapping_file.go
// lets an operator remap every virtual scope, so no descriptor can state the
// virtual JWT scope a deployment will demand (preparation blocker B1). These
// constants are therefore the enforced requirement; under the product's
// DefaultMapping the *Read scopes arrive through ledger:read and the *Write
// scopes through ledger:write, but that mapping is a default, not a contract.
const (
	scopeLedgerRead       = "ledger:LedgerRead"
	scopeLedgerWrite      = "ledger:LedgerWrite"
	scopeTransactionRead  = "ledger:TransactionRead"
	scopeTransactionWrite = "ledger:TransactionWrite"
	scopeAccountRead      = "ledger:AccountRead"
	scopeMetadataWrite    = "ledger:MetadataWrite"
	scopeAuditRead        = "ledger:AuditRead"
	scopeOpsRead          = "ledger:OpsRead"
	scopeOpsWrite         = "ledger:OpsWrite"
	scopeQueryRead        = "ledger:QueryRead"
	scopeQueryWrite       = "ledger:QueryWrite"
)

// Message ceilings. Every value stays inside the host's common generated-client
// profile (sdk.GeneratedClientMaxMessageBytes and friends) and is chosen for
// the shape of the traffic rather than set to the maximum.
const (
	readRequestBytes          int64  = 64 << 10
	applyRequestBytes         int64  = 256 << 10
	configurationRequestBytes int64  = 512 << 10
	unaryResponseBytes        int64  = 512 << 10
	streamMessageBytes        int64  = 256 << 10
	streamAggregateBytes      int64  = 8 << 20
	streamMessages            uint32 = 1024
)

// operation is one product RPC binding: the gRPC method, its shape, and the
// single granular scope the server enforces for it.
type operation struct {
	id        string
	method    string
	streaming bool
	scope     string
	// requestBytes overrides readRequestBytes for operations that carry a
	// materially larger request body.
	requestBytes int64
}

func (o operation) policy() sdk.OperationPolicy {
	request := o.requestBytes
	if request == 0 {
		request = readRequestBytes
	}
	limits := sdk.ResponseLimits{
		MaxMessageBytes:   unaryResponseBytes,
		MaxMessages:       1,
		MaxAggregateBytes: unaryResponseBytes,
	}
	if o.streaming {
		limits = sdk.ResponseLimits{
			MaxMessageBytes:   streamMessageBytes,
			MaxMessages:       streamMessages,
			MaxAggregateBytes: streamAggregateBytes,
		}
	}
	return sdk.OperationPolicy{
		ID:      o.id,
		Service: sdk.ServiceLedger,
		Scopes:  []string{o.scope},
		GRPC: &sdk.GRPCOperationPolicy{
			FullMethod:      o.method,
			ServerStreaming: o.streaming,
			GeneratedClient: &sdk.GRPCGeneratedClientPolicy{
				MaxRequestMessageBytes: request,
				ResponseLimits:         limits,
			},
		},
	}
}

// Read operations. Scopes are those enforced by
// internal/adapter/grpc/server_bucket.go at 9a6fa7d0.
var (
	opListLedgers           = operation{id: "ledger.v3.ListLedgers", method: bucketFullMethod("ListLedgers"), streaming: true, scope: scopeLedgerRead}
	opGetLedger             = operation{id: "ledger.v3.GetLedger", method: bucketFullMethod("GetLedger"), scope: scopeLedgerRead}
	opGetLedgerStats        = operation{id: "ledger.v3.GetLedgerStats", method: bucketFullMethod("GetLedgerStats"), scope: scopeLedgerRead}
	opGetMetadataSchema     = operation{id: "ledger.v3.GetMetadataSchemaStatus", method: bucketFullMethod("GetMetadataSchemaStatus"), scope: scopeAccountRead}
	opGetAccount            = operation{id: "ledger.v3.GetAccount", method: bucketFullMethod("GetAccount"), scope: scopeAccountRead}
	opListAccounts          = operation{id: "ledger.v3.ListAccounts", method: bucketFullMethod("ListAccounts"), streaming: true, scope: scopeAccountRead}
	opAggregateVolumes      = operation{id: "ledger.v3.AggregateVolumes", method: bucketFullMethod("AggregateVolumes"), scope: scopeAccountRead}
	opAnalyzeAccounts       = operation{id: "ledger.v3.AnalyzeAccounts", method: bucketFullMethod("AnalyzeAccounts"), streaming: true, scope: scopeAccountRead}
	opGetTransaction        = operation{id: "ledger.v3.GetTransaction", method: bucketFullMethod("GetTransaction"), scope: scopeTransactionRead}
	opListTransactions      = operation{id: "ledger.v3.ListTransactions", method: bucketFullMethod("ListTransactions"), streaming: true, scope: scopeTransactionRead}
	opAnalyzeTransactions   = operation{id: "ledger.v3.AnalyzeTransactions", method: bucketFullMethod("AnalyzeTransactions"), streaming: true, scope: scopeTransactionRead}
	opGetAuditEntry         = operation{id: "ledger.v3.GetAuditEntry", method: bucketFullMethod("GetAuditEntry"), scope: scopeAuditRead}
	opListAuditEntries      = operation{id: "ledger.v3.ListAuditEntries", method: bucketFullMethod("ListAuditEntries"), streaming: true, scope: scopeAuditRead}
	opListLogs              = operation{id: "ledger.v3.ListLogs", method: bucketFullMethod("ListLogs"), streaming: true, scope: scopeLedgerRead}
	opGetLog                = operation{id: "ledger.v3.GetLog", method: bucketFullMethod("GetLog"), scope: scopeOpsRead}
	opListIndexes           = operation{id: "ledger.v3.ListIndexes", method: bucketFullMethod("ListIndexes"), streaming: true, scope: scopeLedgerRead}
	opInspectIndex          = operation{id: "ledger.v3.InspectIndex", method: bucketFullMethod("InspectIndex"), scope: scopeLedgerRead}
	opGetNumscript          = operation{id: "ledger.v3.GetNumscript", method: bucketFullMethod("GetNumscript"), scope: scopeQueryRead}
	opListNumscripts        = operation{id: "ledger.v3.ListNumscripts", method: bucketFullMethod("ListNumscripts"), streaming: true, scope: scopeQueryRead}
	opListNumscriptVersions = operation{id: "ledger.v3.ListNumscriptVersions", method: bucketFullMethod("ListNumscriptVersions"), scope: scopeQueryRead}
	opListPreparedQueries   = operation{id: "ledger.v3.ListPreparedQueries", method: bucketFullMethod("ListPreparedQueries"), scope: scopeQueryRead}
	opExecutePreparedQuery  = operation{id: "ledger.v3.ExecutePreparedQuery", method: bucketFullMethod("ExecutePreparedQuery"), scope: scopeQueryRead}
)

// applyOperation names one Apply batch shape. Every v3 write travels through
// the single unary /ledger.BucketService/Apply, so the operation identity is
// the batch's request variant, not the method — the server runs
// RequiredScopeForRequest over every request in the batch
// (server_bucket.go Apply), so each distinct variant carries its own scope.
func applyOperation(id, scope string, requestBytes int64) operation {
	return operation{
		id:           "ledger.v3.Apply." + id,
		method:       bucketFullMethod("Apply"),
		scope:        scope,
		requestBytes: requestBytes,
	}
}

var (
	opApplyCreateTransaction    = applyOperation("CreateTransaction", scopeTransactionWrite, applyRequestBytes)
	opApplyRevertTransaction    = applyOperation("RevertTransaction", scopeTransactionWrite, applyRequestBytes)
	opApplyAddMetadata          = applyOperation("AddMetadata", scopeMetadataWrite, applyRequestBytes)
	opApplyDeleteMetadata       = applyOperation("DeleteMetadata", scopeMetadataWrite, applyRequestBytes)
	opApplyCreateLedger         = applyOperation("CreateLedger", scopeLedgerWrite, applyRequestBytes)
	opApplyDeleteLedger         = applyOperation("DeleteLedger", scopeLedgerWrite, applyRequestBytes)
	opApplyCreateIndex          = applyOperation("CreateIndex", scopeLedgerWrite, applyRequestBytes)
	opApplyDropIndex            = applyOperation("DropIndex", scopeLedgerWrite, applyRequestBytes)
	opApplySaveNumscript        = applyOperation("SaveNumscript", scopeLedgerWrite, applyRequestBytes)
	opApplySetMetadataFieldType = applyOperation("SetMetadataFieldType", scopeMetadataWrite, applyRequestBytes)
	opApplyRemoveMetadataType   = applyOperation("RemoveMetadataFieldType", scopeMetadataWrite, applyRequestBytes)
	opApplySaveLedgerMetadata   = applyOperation("SaveLedgerMetadata", scopeMetadataWrite, applyRequestBytes)
	opApplyDeleteLedgerMetadata = applyOperation("DeleteLedgerMetadata", scopeMetadataWrite, applyRequestBytes)
	opApplyAddAccountType       = applyOperation("AddAccountType", scopeMetadataWrite, applyRequestBytes)
	opApplyRemoveAccountType    = applyOperation("RemoveAccountType", scopeMetadataWrite, applyRequestBytes)
	opApplyDefaultEnforcement   = applyOperation("SetDefaultEnforcementMode", scopeMetadataWrite, applyRequestBytes)
	opApplyCreatePreparedQuery  = applyOperation("CreatePreparedQuery", scopeQueryWrite, applyRequestBytes)
	opApplyUpdatePreparedQuery  = applyOperation("UpdatePreparedQuery", scopeQueryWrite, applyRequestBytes)
	opApplyDeletePreparedQuery  = applyOperation("DeletePreparedQuery", scopeQueryWrite, applyRequestBytes)
)

// opApplyConfiguration is the compound declarative apply. Its batch may carry
// every variant ledgers/config_model.go emits, so the declared scope set is the
// union of what those variants require — the server checks each request in the
// batch independently, so anything narrower would be a descriptor that
// under-states the authority the call needs.
var opApplyConfiguration = operation{
	id:           "ledger.v3.Apply.Configuration",
	method:       bucketFullMethod("Apply"),
	scope:        "",
	requestBytes: configurationRequestBytes,
}

// configurationScopes is the sorted union required by a declarative
// configuration apply: account types and metadata field types are
// MetadataWrite, index and numscript changes are LedgerWrite, prepared-query
// changes are QueryWrite.
var configurationScopes = []string{scopeLedgerWrite, scopeMetadataWrite, scopeQueryWrite}
