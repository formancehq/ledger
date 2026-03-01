package readstore

import bolt "go.etcd.io/bbolt"

// WriteAccountExistence records that an account exists in the read store.
func WriteAccountExistence(tx *bolt.Tx, kb *KeyBuilder, ledger, account string) error {
	b := tx.Bucket(BucketExistence)
	key := ExistenceKey(kb, ledger, NamespaceAccount, []byte(account))
	return b.Put(key, nil)
}

// WriteTransactionExistence records that a transaction exists in the read store.
func WriteTransactionExistence(tx *bolt.Tx, kb *KeyBuilder, ledger string, txID uint64) error {
	b := tx.Bucket(BucketExistence)
	entityID := make([]byte, 0, 8)
	entityID = EncodeTxID(entityID, txID)
	key := ExistenceKey(kb, ledger, NamespaceTransaction, entityID)
	return b.Put(key, nil)
}
