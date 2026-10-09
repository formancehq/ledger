package protohelpers

import commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

// Compatibility aliases for protobuf-only builders now implemented with the
// public generated models.
var NewTransaction = commonpb.NewTransaction

var WithTransactionPostings = commonpb.WithTransactionPostings

var WithTransactionID = commonpb.WithTransactionID

var WithTransactionTimestamp = commonpb.WithTransactionTimestamp

var NewLedgerLog = commonpb.NewLedgerLog

var WithLedgerLogID = commonpb.WithLedgerLogID

var WithLedgerLogDate = commonpb.WithLedgerLogDate

// ToLedgerInfo projects the server's create-log payload into an HTTP response.
var ToLedgerInfo = commonpb.ToLedgerInfo
