package ledgerpb

func (state *LedgerState) GetNextTransactionID() uint64 {
	ret := state.NextTransactionId
	state.NextTransactionId++
	return ret
}

func (state *LedgerState) GetNextLogID() uint64 {
	ret := state.NextLogId
	state.NextLogId++
	return ret
}
