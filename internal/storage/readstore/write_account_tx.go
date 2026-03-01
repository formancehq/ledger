package readstore

import bolt "go.etcd.io/bbolt"

// WriteAccountTxMapping records that a transaction involves an account.
func WriteAccountTxMapping(tx *bolt.Tx, kb *KeyBuilder, ledger, account string, txID uint64) error {
	b := tx.Bucket(BucketAccountTx)
	key := AccountTxKey(kb, ledger, account, txID)
	return b.Put(key, nil)
}
