// Package oracletest provides request builders shared by the oracle's own tests
// and the driver's tests, so both construct model inputs identically. Builders
// default to ledger "L"; the *L variants take an explicit ledger.
package oracletest

import (
	"math/big"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func AddTypeReqP(name string, p commonpb.AccountTypePersistence) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_AddAccountType{
			AddAccountType: &commonpb.AddAccountTypeLedgerRequest{
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

func AddTypeReq(name string) *commonpb.Request {
	return AddTypeReqP(name, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL)
}

func RemoveReqL(ledger, name string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RemoveAccountType{
			RemoveAccountType: &commonpb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name},
		},
	}
}

func RemoveTypeReq(name string) *commonpb.Request {
	return RemoveReqL("L", name)
}

func TxReqL(ledger, src, dest, asset string, amount int64) *commonpb.Request {
	return TxReqColoredL(ledger, src, dest, asset, "", amount)
}

// TxReqColoredL is TxReqL on an explicit color bucket; "" is the uncolored one.
func TxReqColoredL(ledger, src, dest, asset, color string, amount int64) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_CreateTransaction{
						CreateTransaction: &commonpb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewColoredPosting(src, dest, asset, color, big.NewInt(amount)),
							},
						},
					},
				},
			},
		},
	}
}

func TxReq(src, dest, asset string, amount int64) *commonpb.Request {
	return TxReqL("L", src, dest, asset, amount)
}

// TxReqForce is TxReq with an explicit Force flag; Force=true skips the balance
// floor (matches the SUT's skipBalanceCheck in applyPosting).
func TxReqForce(src, dest, asset string, amount int64, force bool) *commonpb.Request {
	req := TxReqL("L", src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Force = force

	return req
}

// TxReqRefL is TxReqL carrying a transaction reference, so tests can trigger
// TRANSACTION_REFERENCE_CONFLICT (a second create reusing the same reference).
func TxReqRefL(ledger, ref, src, dest, asset string, amount int64) *commonpb.Request {
	req := TxReqL(ledger, src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Reference = ref

	return req
}

// TxReqMulti builds a multi-posting CreateTransaction (ledger "L") with an
// explicit Force flag. The postings compose in order — an earlier one can fund a
// later one's source within the same transaction.
func TxReqMulti(force bool, postings ...*commonpb.Posting) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_CreateTransaction{
						CreateTransaction: &commonpb.CreateTransactionPayload{Postings: postings, Force: force},
					},
				},
			},
		},
	}
}

// RevertReqL builds a RevertTransaction of txID in ledger. Force=true skips the
// balance floor on the reversed postings (reverts always set it).
func RevertReqL(ledger string, txID uint64, force bool) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_RevertTransaction{
						RevertTransaction: &commonpb.RevertTransactionPayload{TransactionId: txID, Force: force},
					},
				},
			},
		},
	}
}

// AddTxMetaReq builds an AddMetadata targeting transaction txID (ledger "L").
func AddTxMetaReq(txID uint64, md map[string]*commonpb.MetadataValue) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_AddMetadata{
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

// SetFieldTypeReq declares (target, key) with the given metadata type on
// ledger "L".
func SetFieldTypeReq(target commonpb.TargetType, key string, t commonpb.MetadataType) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeRequest{
				Ledger:     "L",
				TargetType: target,
				Key:        key,
				Type:       t,
			},
		},
	}
}

// RemoveFieldTypeReq drops the (target, key) declaration on ledger "L".
func RemoveFieldTypeReq(target commonpb.TargetType, key string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &commonpb.RemoveMetadataFieldTypeRequest{
				Ledger:     "L",
				TargetType: target,
				Key:        key,
			},
		},
	}
}

// CreateIndexReq creates an index on ledger "L".
func CreateIndexReq(id *commonpb.IndexID) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateIndex{
			CreateIndex: &commonpb.CreateIndexRequest{Ledger: "L", Id: id},
		},
	}
}

// DropIndexReq drops an index on ledger "L".
func DropIndexReq(id *commonpb.IndexID) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DropIndex{
			DropIndex: &commonpb.DropIndexRequest{Ledger: "L", Id: id},
		},
	}
}

// AddAccountMetaReq writes one metadata value on an account of ledger "L".
func AddAccountMetaReq(addr, key string, v *commonpb.MetadataValue) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target:   &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}}},
							Metadata: map[string]*commonpb.MetadataValue{key: v},
						},
					},
				},
			},
		},
	}
}
