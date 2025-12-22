package ledgerpb

// GetMemento returns the memento structure for CreatedTransaction
// Excludes postCommitVolumes and postCommitEffectiveVolumes fields from transactions
func (ct *CreatedTransaction) GetMemento() *CreatedTransactionMemento {
	if ct == nil {
		return nil
	}

	tx := ct.Transaction
	if tx == nil {
		return &CreatedTransactionMemento{
			AccountMetadata: ct.AccountMetadata,
		}
	}

	// Convert Transaction to TransactionResume (excluding volumes)
	// Note: Reverted field is not set in CreatedTransaction memento (will be false by default)
	transactionResume := &TransactionResume{
		Postings:  tx.Postings,
		Metadata:  tx.Metadata,
		Timestamp: tx.Timestamp,
		Reference: tx.Reference,
		// Reverted is not set - will default to false
	}

	// Set ID if present (0 means nil in protobuf)
	if tx.Id != 0 {
		transactionResume.Id = tx.Id
	} else {
		transactionResume.Id = 0 // Explicitly set to 0 to indicate nil
	}

	return &CreatedTransactionMemento{
		Transaction:     transactionResume,
		AccountMetadata: ct.AccountMetadata,
	}
}

// GetMemento returns the memento structure for RevertedTransaction
func (rt *RevertedTransaction) GetMemento() *RevertedTransactionMemento {
	if rt == nil {
		return nil
	}

	// RevertTransaction is required, so we can safely access it
	revertTx := rt.RevertTransaction

	// Convert RevertTransaction to TransactionResume
	// Note: Reverted field is not set in RevertedTransaction memento (will be false by default)
	transactionResume := &TransactionResume{
		Postings:  revertTx.Postings,
		Metadata:  revertTx.Metadata,
		Timestamp: revertTx.Timestamp,
		Reference: revertTx.Reference,
		// Reverted is not set - will default to false
	}

	// Set ID if present
	if revertTx.Id != 0 {
		transactionResume.Id = revertTx.Id
	} else {
		transactionResume.Id = 0 // Explicitly set to 0 to indicate nil
	}

	// Get reverted transaction ID
	var revertedTxID uint64
	if rt.RevertedTransaction != nil && rt.RevertedTransaction.Id != 0 {
		revertedTxID = rt.RevertedTransaction.Id
	}

	return &RevertedTransactionMemento{
		RevertedTransactionId: revertedTxID,
		RevertTransaction:     transactionResume,
	}
}

// GetMemento returns the memento structure for SavedMetadata
// For SavedMetadata, the memento is the same as the structure itself
func (sm *SavedMetadata) GetMemento() *SavedMetadata {
	if sm == nil {
		return nil
	}
	return sm
}

// GetMemento returns the memento structure for DeletedMetadata
// For DeletedMetadata, the memento is the same as the structure itself
func (dm *DeletedMetadata) GetMemento() *DeletedMetadata {
	if dm == nil {
		return nil
	}
	return dm
}
