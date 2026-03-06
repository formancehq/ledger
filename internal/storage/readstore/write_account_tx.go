package readstore

import (
	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// WriteAccountTxMapping records that a transaction involves an account (any role).
func WriteAccountTxMapping(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	b := tx.Bucket(BucketAccountTx)
	key := AccountTxKey(kb, ledger, account, txID)

	return b.Put(key, nil)
}

// WriteSourceAccountTxMapping records that an account is a source in a transaction.
func WriteSourceAccountTxMapping(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	b := tx.Bucket(BucketSourceAccountTx)
	key := AccountTxKey(kb, ledger, account, txID)

	return b.Put(key, nil)
}

// WriteDestAccountTxMapping records that an account is a destination in a transaction.
func WriteDestAccountTxMapping(tx *bolt.Tx, kb *dal.KeyBuilder, ledger, account string, txID uint64) error {
	b := tx.Bucket(BucketDestAccountTx)
	key := AccountTxKey(kb, ledger, account, txID)

	return b.Put(key, nil)
}
