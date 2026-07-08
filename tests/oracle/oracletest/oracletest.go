// Package oracletest provides request builders shared by the oracle's own tests
// and the driver's tests, so both construct model inputs identically. Builders
// default to ledger "L"; the *L variants take an explicit ledger.
package oracletest

import (
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func AddTypeReqP(name string, p commonpb.AccountTypePersistence) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: "L",
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     name + ":{id}",
					Persistence: p,
				},
			},
		},
	}
}

func AddTypeReq(name string) *servicepb.Request {
	return AddTypeReqP(name, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL)
}

func RemoveReqL(ledger, name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name},
		},
	}
}

func RemoveTypeReq(name string) *servicepb.Request {
	return RemoveReqL("L", name)
}

func TxReqL(ledger, src, dest, asset string, amount int64) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting(src, dest, asset, big.NewInt(amount)),
							},
						},
					},
				},
			},
		},
	}
}

func TxReq(src, dest, asset string, amount int64) *servicepb.Request {
	return TxReqL("L", src, dest, asset, amount)
}

// TxReqForce is TxReq with an explicit Force flag; Force=true skips the balance
// floor (matches the SUT's skipBalanceCheck in applyPosting).
func TxReqForce(src, dest, asset string, amount int64, force bool) *servicepb.Request {
	req := TxReqL("L", src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Force = force

	return req
}

// TxReqRefL is TxReqL carrying a transaction reference, so tests can trigger
// TRANSACTION_REFERENCE_CONFLICT (a second create reusing the same reference).
func TxReqRefL(ledger, ref, src, dest, asset string, amount int64) *servicepb.Request {
	req := TxReqL(ledger, src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Reference = ref

	return req
}

// TxReqMulti builds a multi-posting CreateTransaction (ledger "L") with an
// explicit Force flag. The postings compose in order — an earlier one can fund a
// later one's source within the same transaction.
func TxReqMulti(force bool, postings ...*commonpb.Posting) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "L",
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{Postings: postings, Force: force},
					},
				},
			},
		},
	}
}

// RevertReqL builds a RevertTransaction of txID in ledger. Force=true skips the
// balance floor on the reversed postings (reverts always set it).
func RevertReqL(ledger string, txID uint64, force bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_RevertTransaction{
						RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: txID, Force: force},
					},
				},
			},
		},
	}
}

// SetTxFieldTypeReq declares a transaction-metadata field type, so tests can
// build a ledger whose only state is its transaction schema.
func SetTxFieldTypeReq(ledger, key string, t commonpb.MetadataType) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:        key,
				Type:       t,
			},
		},
	}
}

// AddTxMetaReq builds an AddMetadata targeting transaction txID (ledger "L").
func AddTxMetaReq(txID uint64, md map[string]*commonpb.MetadataValue) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "L",
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target:   &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: txID}},
							Metadata: md,
						},
					},
				},
			},
		},
	}
}
