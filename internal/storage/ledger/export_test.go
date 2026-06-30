package ledger

func (store *Store) ResetIndexedMetadataKeysForTest() {
	store.indexedKeysResolved = false
	store.indexedMetadataKeys = nil
}
