// Package oracletest provides request builders shared by the oracle's own tests
// and the driver's tests, so both construct model inputs identically. Builders
// default to ledger "L"; the *L variants take an explicit ledger.
package oracletest

import (
	"math/big"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

func AddTypeReqP(name string, p ledgerpb.AccountTypePersistence) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_AddAccountType{
			AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{
				Ledger: "L",
				AccountType: &ledgerpb.AccountType{
					Name:        name,
					Pattern:     name + ":{id}",
					Persistence: p,
				},
			},
		},
	}
}

func AddTypeReq(name string) *ledgerpb.Request {
	return AddTypeReqP(name, ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL)
}

func RemoveReqL(ledger, name string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveAccountType{
			RemoveAccountType: &ledgerpb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name},
		},
	}
}

func RemoveTypeReq(name string) *ledgerpb.Request {
	return RemoveReqL("L", name)
}

func TxReqL(ledger, src, dest, asset string, amount int64) *ledgerpb.Request {
	return TxReqColoredL(ledger, src, dest, asset, "", amount)
}

// TxReqColoredL is TxReqL on an explicit color bucket; "" is the uncolored one.
func TxReqColoredL(ledger, src, dest, asset, color string, amount int64) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{
							Postings: []*ledgerpb.Posting{
								protohelpers.NewColoredPosting(src, dest, asset, color, big.NewInt(amount)),
							},
						},
					},
				},
			},
		},
	}
}

func TxReq(src, dest, asset string, amount int64) *ledgerpb.Request {
	return TxReqL("L", src, dest, asset, amount)
}

// TxReqForce is TxReq with an explicit Force flag; Force=true skips the balance
// floor (matches the SUT's skipBalanceCheck in applyPosting).
func TxReqForce(src, dest, asset string, amount int64, force bool) *ledgerpb.Request {
	req := TxReqL("L", src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Force = force

	return req
}

// TxReqRefL is TxReqL carrying a transaction reference, so tests can trigger
// TRANSACTION_REFERENCE_CONFLICT (a second create reusing the same reference).
func TxReqRefL(ledger, ref, src, dest, asset string, amount int64) *ledgerpb.Request {
	req := TxReqL(ledger, src, dest, asset, amount)
	req.GetApply().GetAction().GetCreateTransaction().Reference = ref

	return req
}

// TxReqMulti builds a multi-posting CreateTransaction (ledger "L") with an
// explicit Force flag. The postings compose in order — an earlier one can fund a
// later one's source within the same transaction.
func TxReqMulti(force bool, postings ...*ledgerpb.Posting) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_CreateTransaction{
						CreateTransaction: &ledgerpb.CreateTransactionPayload{Postings: postings, Force: force},
					},
				},
			},
		},
	}
}

// RevertReqL builds a RevertTransaction of txID in ledger. Force=true skips the
// balance floor on the reversed postings (reverts always set it).
func RevertReqL(ledger string, txID uint64, force bool) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_RevertTransaction{
						RevertTransaction: &ledgerpb.RevertTransactionPayload{TransactionId: txID, Force: force},
					},
				},
			},
		},
	}
}

// AddTxMetaReq builds an AddMetadata targeting transaction txID (ledger "L").
func AddTxMetaReq(txID uint64, md map[string]*ledgerpb.MetadataValue) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_AddMetadata{
						AddMetadata: &ledgerpb.SaveMetadataCommand{
							Target:   &ledgerpb.Target{Target: &ledgerpb.Target_TransactionId{TransactionId: txID}},
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
func SetFieldTypeReq(target ledgerpb.TargetType, key string, t ledgerpb.MetadataType) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeRequest{
				Ledger:     "L",
				TargetType: target,
				Key:        key,
				Type:       t,
			},
		},
	}
}

// RemoveFieldTypeReq drops the (target, key) declaration on ledger "L".
func RemoveFieldTypeReq(target ledgerpb.TargetType, key string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &ledgerpb.RemoveMetadataFieldTypeRequest{
				Ledger:     "L",
				TargetType: target,
				Key:        key,
			},
		},
	}
}

// CreateIndexReq creates an index on ledger "L".
func CreateIndexReq(id *ledgerpb.IndexID) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateIndex{
			CreateIndex: &ledgerpb.CreateIndexRequest{Ledger: "L", Id: id},
		},
	}
}

// DropIndexReq drops an index on ledger "L".
func DropIndexReq(id *ledgerpb.IndexID) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DropIndex{
			DropIndex: &ledgerpb.DropIndexRequest{Ledger: "L", Id: id},
		},
	}
}

// AddAccountMetaReq writes one metadata value on an account of ledger "L".
func AddAccountMetaReq(addr, key string, v *ledgerpb.MetadataValue) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "L",
				Action: &ledgerpb.LedgerAction{
					Data: &ledgerpb.LedgerAction_AddMetadata{
						AddMetadata: &ledgerpb.SaveMetadataCommand{
							Target:   &ledgerpb.Target{Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{Addr: addr}}},
							Metadata: map[string]*ledgerpb.MetadataValue{key: v},
						},
					},
				},
			},
		},
	}
}
